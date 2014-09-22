require 'thread'

Thread.abort_on_exception = true

module Derrick
  class Inspector
    attr_reader :redis, :progress

    def initialize(redis, context)
      @redis = redis
      @context = context
      @progress = Progress.new(@redis.dbsize)
    end

    def report
      keys_queue = Queue.new
      stats_queue = Queue.new
      Thread.new { Collector.new(@redis, keys_queue, @progress, @context).run }
      @context.concurrency.times do
        Thread.new { Fetcher.new(@redis, keys_queue, stats_queue, @progress).run }
      end
      aggregator = Aggregator.new(stats_queue, @context)
      aggregator.run
      aggregator.patterns
    end
  end
end