module Derrick
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
end
