require 'optparse'
require 'redis'

require 'byebug'

module Derrick
  class CLI
    class ProgressDisplay

      def initialize(progress, output)
        @progress = progress
        @output = output
        @last_output_size = 0
      end

      def render
        clear
        message = "collected: #{@progress.collected}/#{@progress.total} fetched: #{@progress.fetched}/#{@progress.total}"
        @last_output_size = message.size
        @output.print message
      end

      def clear
        @output.print "\b" * @last_output_size
      end

      def start
        @thread = Thread.new do
          loop do
            render
            sleep 1
          end
        end
      end

      def stop
        @thread.kill
        clear
      end

    end

    class Context
      attr_accessor :concurrency, :batch_size, :max_patterns

      def initialize
        @concurrency = 2
        @batch_size = 10_000
        @max_patterns = 1_000
      end
    end

    class AggregateFormatter
      def initialize(aggregate)
        @aggregate = Hash[aggregate.sort_by { |p, a| -a.count }]
      end

      def each
        yield render_header
        @aggregate.each { |n, s| yield render_line(n, s) }
      end

      def render_header
        [
          'Pattern'.ljust(key_size),
          'Count'.rjust(6),
          'Exp'.rjust(4),
          'Type',
        ].join(' ')
      end

      def render_line(name, stats)
        [
          name.ljust(key_size),
          stats.count.to_s.rjust(6),
          "#{(stats.expirable_ratio * 100).round}%".rjust(4),
          types_summary(stats)
        ].join(' ')
      end

      def types_summary(stats)
        if stats.types_count.size == 1
          stats.types_count.keys.first
        else
          stats.types_ratio.map do |type, ratio|
            "#{type}: #{(ratio * 100).round}%"
          end.join(',')
        end
      end

      def key_size
        @key_size ||= @aggregate.keys.map(&:size).max
      end
    end

    def self.run(args)
      new(args).run!
    end

    def initialize(args)
      @context = Context.new
      @command, *@arguments = parser.parse(args)
    end

    def run!
      abort! unless @command
      public_send("command_#{@command}", *@arguments)
    end

    def command_inspect(database_url=nil)
      inspector = Derrick::Inspector.new(Redis.new(url: database_url), @context)

      progress_display = ProgressDisplay.new(inspector.progress, STDERR)
      progress_display.start

      aggregate = inspector.report
      progress_display.stop

      AggregateFormatter.new(aggregate).each do |line|
        puts line
      end
    end

    protected

    def abort!(message=parser.help)
      puts message
      exit 1
    end

    def parser
      OptionParser.new do |opts|
        opts.banner = "Inpect Redis databases to compute statistics on keys"
        opts.separator ""
        opts.separator "Usage: derrick inspect [options] DATABASE_URL"
        opts.separator ""
        opts.separator "Main options:"
        opts.on('-c', '--concurrency CONCURRENCY') do |concurrency|
          @context.concurrency = Integer(concurrency)
        end
        opts.on('-b', '--batch-size BATCH_SIZE') do |batch_size|
          @context.batch_size = Integer(batch_size)
        end
        opts.on('-C', '--max-patterns MAX_PATTERNS') do |max_patterns|
          @context.max_patterns = Integer(max_patterns)
        end
      end
    end

  end
end