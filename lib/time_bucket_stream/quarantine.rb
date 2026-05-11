# frozen_string_literal: true

require "fileutils"
require "json"
require "time"

class TimeBucketStream
  class Quarantine
    DEFAULT_RETENTION = 7 * 24 * 60 * 60
    TIMESTAMP_PATTERN = /\.q-(\d{20})-\d+-[0-9a-f]+\.jsonl\z/

    attr_reader :paths, :retention

    def self.normalize_retention(value)
      return nil if value.nil?

      seconds = Integer(value)
      return seconds if seconds >= 0

      raise ArgumentError
    rescue ArgumentError, TypeError, RangeError
      raise ArgumentError, "quarantine_retention must be nil or a non-negative number of seconds"
    end

    def initialize(path:, clock: Time, retention: DEFAULT_RETENTION)
      @paths = Paths.new(path: path)
      @clock = clock
      @retention = self.class.normalize_retention(retention)
    end

    def cleanup
      return unless retention

      cutoff = current_time.utc - retention

      quarantine_log_names.each do |log_name|
        delete_pair(log_name) if expired_log?(log_name, cutoff)
      end

      orphan_metadata_names.each do |metadata_name|
        delete_metadata(metadata_name) if expired_metadata?(metadata_name, cutoff)
      end
    rescue SystemCallError, IOError, JSON::ParserError
      nil
    end

    private

    def quarantine_log_names
      children.select { |name| name.end_with?(".jsonl") }.sort
    end

    def metadata_names
      children.select { |name| name.end_with?(".meta.json") }.sort
    end

    def orphan_metadata_names
      metadata_names.reject do |metadata_name|
        File.exist?(quarantine_log_path(metadata_name.delete_suffix(".meta.json")))
      end
    end

    def children
      Dir.children(paths.quarantine)
    rescue SystemCallError
      []
    end

    def expired_log?(log_name, cutoff)
      quarantine_time_for(log_name) < cutoff
    end

    def expired_metadata?(metadata_name, cutoff)
      log_name = metadata_name.delete_suffix(".meta.json")
      quarantine_time_for(log_name, metadata_name: metadata_name) < cutoff
    end

    def quarantine_time_for(log_name, metadata_name: "#{log_name}.meta.json")
      metadata_time(metadata_path(metadata_name)) ||
        timestamp_from_name(log_name) ||
        file_time(quarantine_log_path(log_name)) ||
        file_time(metadata_path(metadata_name)) ||
        current_time.utc
    end

    def metadata_time(path)
      return unless File.exist?(path)

      value = JSON.parse(File.read(path)).fetch("quarantined_at", nil)
      Time.parse(value).utc if value
    rescue SystemCallError, IOError, JSON::ParserError, TypeError, ArgumentError
      nil
    end

    def timestamp_from_name(log_name)
      match = log_name.match(TIMESTAMP_PATTERN)
      parse_timestamp(match[1]) if match
    rescue ArgumentError
      nil
    end

    def parse_timestamp(value)
      Time.utc(
        value[0, 4].to_i,
        value[4, 2].to_i,
        value[6, 2].to_i,
        value[8, 2].to_i,
        value[10, 2].to_i,
        value[12, 2].to_i,
        value[14, 6].to_i
      )
    end

    def file_time(path)
      File.mtime(path).utc
    rescue SystemCallError, IOError
      nil
    end

    def delete_pair(log_name)
      FileUtils.rm_f(quarantine_log_path(log_name))
      FileUtils.rm_f(metadata_path("#{log_name}.meta.json"))
    end

    def delete_metadata(metadata_name)
      FileUtils.rm_f(metadata_path(metadata_name))
    end

    def quarantine_log_path(log_name)
      paths.quarantine_log_for(log_name)
    end

    def metadata_path(metadata_name)
      File.join(paths.quarantine, metadata_name)
    end

    def current_time
      @clock.respond_to?(:call) ? @clock.call : @clock.now
    end
  end
end
