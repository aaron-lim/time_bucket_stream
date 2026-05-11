# frozen_string_literal: true

require "fileutils"
require "securerandom"
require "socket"

class TimeBucketStream
  class Writer
    BUCKET_FORMAT = "%Y%m%d%H%M"
    SYNC_MODES = %i[none flush fsync].freeze

    attr_reader :codec, :paths, :sync

    def initialize(path:, sync: :flush, clock: Time, codec: Codecs::Json.new)
      @paths = Paths.new(path: path)
      @sync = sync.respond_to?(:to_sym) ? sync.to_sym : sync
      @clock = clock
      @codec = Codecs.validate!(codec)
      @mutex = Mutex.new

      validate_sync!
      ensure_directories
    end

    def append(payload)
      @mutex.synchronize do
        prepare_writer
        @sequence += 1

        entry = {
          "id" => @sequence,
          "payload" => payload
        }

        write_line(encode_entry(entry))
        entry.fetch("id")
      end
    end

    def close
      @mutex.synchronize do
        close_writer
      end
    end

    def close_stale
      bucket = current_bucket

      @mutex.synchronize do
        close_writer if @writer_bucket && @writer_bucket < bucket
      end
    end

    private

    def validate_sync!
      return if SYNC_MODES.include?(sync)

      raise ArgumentError, "unsupported file sync mode: #{sync.inspect}"
    end

    def prepare_writer
      bucket = current_bucket
      return if writer_ready?(bucket)

      close_writer
      ensure_directories

      @writer_pid = Process.pid
      @writer_bucket = bucket
      @writer_id = "#{bucket}-#{safe_hostname}-#{Process.pid}-#{SecureRandom.hex(4)}"
      @sequence = 0
      @log_path = paths.log_for(@writer_id)
      @file = File.open(@log_path, File::WRONLY | File::CREAT | File::APPEND, 0o600)
    end

    def encode_entry(entry)
      encoded = codec.dump(entry)
      return encoded if encoded.is_a?(String) && !encoded.match?(/[\r\n]/)

      raise ArgumentError, "codec must dump each entry to one line"
    end

    def writer_ready?(bucket)
      active_writer? &&
        @writer_bucket == bucket &&
        log_file_exists?(@log_path)
    end

    def active_writer?
      @writer_pid == Process.pid &&
        @file &&
        !@file.closed? &&
        @log_path
    end

    def close_writer
      log_path = @log_path

      close_file(@file)
      @file = nil
      delete_empty_log(log_path)
    end

    def write_line(line)
      data = "#{line}\n"

      if sync == :none
        syswrite_all(data)
        return
      end

      write_all(data)
      @file.flush
      @file.fsync if sync == :fsync
    end

    def write_all(data)
      bytes_written = 0

      while bytes_written < data.bytesize
        chunk = data.byteslice(bytes_written, data.bytesize - bytes_written)
        written = @file.write(chunk)

        raise IOError, "file write returned nil" unless written
        raise IOError, "file write made no progress" unless written.positive?

        bytes_written += written
      end
    end

    def syswrite_all(data)
      bytes_written = 0

      while bytes_written < data.bytesize
        chunk = data.byteslice(bytes_written, data.bytesize - bytes_written)
        written = @file.syswrite(chunk)

        raise IOError, "file syswrite made no progress" unless written.positive?

        bytes_written += written
      end
    end

    def ensure_directories
      FileUtils.mkdir_p(paths.logs)
    end

    def current_bucket
      current_time.utc.strftime(BUCKET_FORMAT)
    end

    def current_time
      @clock.respond_to?(:call) ? @clock.call : @clock.now
    end

    def safe_hostname
      value = Socket.gethostname.gsub(/[^A-Za-z0-9_.-]/, "_")
      value.empty? ? "unknown-host" : value
    rescue
      "unknown-host"
    end

    def log_file_exists?(path)
      File.exist?(path)
    rescue SystemCallError
      false
    end

    def close_file(file)
      file&.close
    rescue SystemCallError, IOError
      nil
    end

    def delete_empty_log(path)
      return unless path && File.exist?(path) && File.zero?(path)

      FileUtils.rm_f(path)
    rescue SystemCallError, IOError
      nil
    end
  end
end
