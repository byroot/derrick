require 'thread'

Thread.abort_on_exception = true

module Derrick
  class Collector
    def initialize(redis, queue, context)
      @redis = redis
      @queue = queue
      @context = context
    end

    def run
      collect_keys
      @context.concurrency.times { @queue.push(:stop) }
    end

    def collect_keys
      cursor = '0'
      loop do
        cursor, keys = @redis.scan(cursor, count: @context.batch_size)
        @queue.push(keys)
        return if cursor == '0'
      end
    end
  end

  Key = Struct.new(:name, :type, :ttl)

  class Fetcher

    def initialize(redis, input, output)
      @redis = redis
      @input = input
      @output = output
    end

    def run
      while (keys = @input.pop) != :stop
        @output.push(stats(keys))
      end
      @output.push(:stop)
    end

    def stats(keys)
      types = @redis.pipelined do
        keys.each do |key|
          @redis.type(key)
        end
      end

      ttls = @redis.pipelined do
        keys.each do |key|
          @redis.ttl(key)
        end
      end

      keys.map.with_index do |key, index|
        Key.new(key, types[index], ttls[index])
      end
    end

  end

  class Pattern
    attr_reader :pattern, :count, :expirable_count, :persisted_count, :types_count

    def initialize
      @count = 0
      @expirable_count = 0
      @persisted_count = 0
      @types_count = Hash.new(0)
    end

    def expirable_ratio
      return 1 if count == 0
      expirable_count.to_f / count
    end

    def types_ratio
      Hash[@types_count.map do |type, sub_count|
        [type, sub_count.to_f / count]
      end]
    end

    def aggregate(key)
      @count += 1

      if key.ttl == -1
        @persisted_count += 1
      else
        @expirable_count += 1
      end

      @types_count[key.type] += 1
    end

  end

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
      canonical_key = key.name.gsub(/(^|:)(\d+|[0-9a-f]{32,40})($|:)/, '\1*\3')
      @patterns[canonical_key] ||= Pattern.new
    end

  end

  class Inspector
    attr_reader :redis

    def initialize(redis, context)
      @redis = redis
      @context = context
    end

    def report
      keys_queue = Queue.new
      stats_queue = Queue.new
      Thread.new { Collector.new(@redis, keys_queue, @context).run }
      @context.concurrency.times do
        Thread.new { Fetcher.new(@redis, keys_queue, stats_queue).run }
      end
      aggregator = Aggregator.new(stats_queue, @context)
      aggregator.run
      aggregator.patterns
    end
  end
end