module Derrick
  class Aggregator
    ANY = '*'.freeze

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

      if patterns.size > @context.max_patterns
        compact_uniques!
      end
    end

    def pattern_from(key)
      @patterns[Pattern.extract(key.name)] ||= Pattern.new
    end

    def compact_uniques!
      any = @patterns.delete(ANY) || Pattern.new
      @patterns.each do |key, aggregate|
        if aggregate.count == 1
          any.merge!(@patterns.delete(key))
        end
      end
      @patterns[ANY] = any
      nil
    end

  end
end
