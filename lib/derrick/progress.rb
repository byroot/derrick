module Derrick
  class Progress
    attr_reader :total, :collected, :fetched

    def initialize(total)
      @total = total
      @mutex = Mutex.new

      @collected = 0
      @fetched = 0
    end

    def increment_collected(count)
      @mutex.synchronize { @collected += count }
    end

    def increment_fetched(count)
      @mutex.synchronize { @fetched += count }
    end
  end
end
