# frozen_string_literal: true

class TimeBucketStream
  module Codecs
    class Oj
      def initialize
        require "oj"
      rescue LoadError
        raise LoadError, 'TimeBucketStream::Codecs::Oj requires `gem "oj"`'
      end

      def dump(value)
        ::Oj.dump(value, mode: :compat)
      end

      def load(value)
        ::Oj.load(value, mode: :compat)
      end
    end
  end
end
