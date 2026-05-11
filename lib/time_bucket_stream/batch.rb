# frozen_string_literal: true

class TimeBucketStream
  class Batch
    include Enumerable

    attr_reader :entries

    def initialize(entries:, on_delete:, on_release:)
      @entries = entries.map { |id, payload| [id, payload].freeze }.freeze
      @on_delete = on_delete
      @on_release = on_release
      @finished = false
    end

    def each
      return enum_for(:each) unless block_given?

      entries.each { |_id, payload| yield payload }
    end

    def size
      entries.size
    end
    alias_method :length, :size

    def empty?
      entries.empty?
    end

    def delete
      finish_with(@on_delete)
    end

    def release
      finish_with(@on_release)
    end

    def finished?
      @finished
    end

    private

    def ids
      entries.map(&:first)
    end

    def finish_with(callback)
      return false if finished?

      callback.call(ids)
      @finished = true
    end
  end
end
