module Derrick
  Key = Struct.new(:name, :type, :ttl)

  class Fetcher
    def initialize(redis, input, output, progress)
      @redis = redis
      @input = input
      @output = output
      @progress = progress
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

      @progress.increment_fetched(keys.size)

      keys.map.with_index do |key, index|
        Key.new(key, types[index], ttls[index])
      end
    end
  end
end
