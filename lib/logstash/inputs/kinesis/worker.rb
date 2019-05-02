# encoding: utf-8
require 'java'

java_import java.io.ByteArrayInputStream
java_import java.io.ByteArrayOutputStream
java_import java.util.zip.GZIPInputStream
java_import java.util.zip.ZipException

class LogStash::Inputs::Kinesis::Worker
  include com.amazonaws.services.kinesis.clientlibrary.interfaces.v2::IRecordProcessor

  attr_reader(
    :checkpoint_interval,
    :codec,
    :decorator,
    :logger,
    :output_queue,
  )

  def initialize(*args)
    # nasty hack, because this is the name of a method on IRecordProcessor, but also ruby's constructor
    if !@constructed
      @codec, @output_queue, @decorator, @checkpoint_interval, @logger = args
      @next_checkpoint = Time.now - 600
      @constructed = true
    else
      _shard_id = args[0].shardId
    end
  end
  public :initialize

  def processRecords(records_input)
    records_input.records.each { |record| process_record(record) }
    if Time.now >= @next_checkpoint
      checkpoint(records_input.checkpointer)
      @next_checkpoint = Time.now + @checkpoint_interval
    end
  end

  def shutdown(shutdown_input)
    if shutdown_input.shutdown_reason == com.amazonaws.services.kinesis.clientlibrary.lib.worker::ShutdownReason::TERMINATE
      checkpoint(shutdown_input.checkpointer)
    end
  end

  protected

  def checkpoint(checkpointer)
    checkpointer.checkpoint()
  rescue => error
    @logger.error("Kinesis worker failed checkpointing: #{error}")
  end

  def process_record(record)
    data = record.getData.array
    # Decompress here
    gzip_header = (data[0] & 0xff) | ((data[1] << 8) & 0xff00)

    if gzip_header == 0x8b1f
      byte_stream = ByteArrayInputStream.new(data)
      gzip_stream = GZIPInputStream.new(byte_stream)
      out_stream = ByteArrayOutputStream.new
      buf = Java::byte[1024].new
      while ((len = gzip_stream.read(buf)) > 0)
        out_stream.write(buf, 0, len)
      end
      raw = String.from_java_bytes(out_stream.toByteArray)
    else
      raw = String.from_java_bytes(data)
    end

    metadata = build_metadata(record)
    @codec.decode(raw) do |event|
      @decorator.call(event)
      event.set('@metadata', metadata)
      @output_queue << event
    end
  rescue => error
    @logger.error("Error processing record: #{error}")
  ensure
    out_stream.close unless out_stream.nil?
    gzip_stream.close unless gzip_stream.nil?
    byte_stream.close unless byte_stream.nil?
  end

  def build_metadata(record)
    metadata = Hash.new
    metadata['approximate_arrival_timestamp'] = record.getApproximateArrivalTimestamp.getTime
    metadata['partition_key'] = record.getPartitionKey
    metadata['sequence_number'] = record.getSequenceNumber
    metadata
  end

end
