module Derrick
  class Pattern
    SEGMENT_SEPARATORS = %w(: _ /).freeze
    FIRST_SEGMENT_PATTERN = /.*(#{SEGMENT_SEPARATORS.map { |s| Regexp.escape(s) }.join('|')})/
    IDENTIFIER_PATTERNS = '\d+|[0-9a-f]{32,40}'
    SEGMENT_PATTERNS = SEGMENT_SEPARATORS.map do |separator|
      /(^|#{Regexp.escape(separator)})(#{IDENTIFIER_PATTERNS})($|#{Regexp.escape(separator)})/
    end

    attr_reader :pattern, :count, :expirable_count, :persisted_count, :types_count

    def self.extract(key_name)
      key_pattern = SEGMENT_PATTERNS.inject(key_name.inspect[1..-2]) do |key, pattern|
        key.gsub(pattern, '\1*\3')
      end

      return "#{key_name[FIRST_SEGMENT_PATTERN]}*" if key_pattern == key_name

      key_pattern
    end

    def initialize
      @count = 0
      @expirable_count = 0
      @persisted_count = 0
      @types_count = Hash.new(0)
    end

    def merge!(other)
      @count += other.count
      @expirable_count += other.expirable_count
      @persisted_count += other.persisted_count
      other.types_count.each do |type, count|
        @types_count[type] += count
      end
      self
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
