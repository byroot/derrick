module Derrick
  class Collector
    def initialize(redis, queue, progress, context)
      @redis = redis
      @queue = queue
      @progress = progress
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
        @progress.increment_collected(keys.size)
        return if cursor == '0'
      end
    end
  end
end
