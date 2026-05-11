# frozen_string_literal: true

require "json"

class TimeBucketStream
  module Codecs
    class Json
      def dump(value)
        JSON.generate(value)
      end

      def load(value)
        JSON.parse(value)
      end
    end
  end
end
