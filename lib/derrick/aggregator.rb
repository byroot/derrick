module Derrick
  class Aggregator
    attr_reader :patterns
    def initialize(queue, context)
      @queue = queue
      @patterns = {}
      @context = context
    end

    def run
      fetcher_count = @context.concurrency
      loop do
        keys = @queue.pop
        if keys == :stop
          fetcher_count -= 1
          break if fetcher_count == 0
        else
          keys.each { |k| aggregate(k) }
        end
      end
      self
    end

    def aggregate(key)
      pattern = pattern_from(key)
      pattern.aggregate(key)
    end

    def pattern_from(key)
      @patterns[Pattern.extract(key.name)] ||= Pattern.new
    end
  end
end
