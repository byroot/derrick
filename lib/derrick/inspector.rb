require 'thread'

Thread.abort_on_exception = true

module Derrick
  BATCH_SIZE = 10
  CONCURENCY = 1

  class Collector
    def initialize(redis, queue)
      @redis = redis
      @queue = queue
    end

    def run
      collect_keys
      CONCURENCY.times { @queue.push(:stop) }
    end

    def collect_keys
      cursor = '0'
      loop do
        cursor, keys = @redis.scan(cursor, count: BATCH_SIZE)
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
    def initialize(queue)
      @queue = queue
      @patterns = {}
    end

    def run
      fetcher_count = CONCURENCY
      loop do
        keys = @queue.pop
        if keys == :stop
          fetcher_count -= 1
          break
        end
        keys.each { |k| aggregate(k) }
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

    def initialize(redis)
      @redis = redis
      @threads = []
    end

    def report
      keys_queue = Queue.new
      stats_queue = Queue.new
      @threads << Thread.new { Collector.new(@redis, keys_queue).run }
      CONCURENCY.times do
        @threads << Thread.new { Fetcher.new(@redis, keys_queue, stats_queue).run }
      end
      aggregator = Aggregator.new(stats_queue)
      aggregator.run
      aggregator.patterns
    end
  end
end