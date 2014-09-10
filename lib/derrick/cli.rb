require 'optparse'
require 'redis'

require 'byebug'

module Derrick
  class CLI
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
      @command, *@arguments = parser.parse(args)
    end

    def run!
      abort! unless @command
      public_send("command_#{@command}", *@arguments)
    end

    def command_inspect(database_url=nil)
      inspector = Derrick::Inspector.new(Redis.new(url: database_url))
      aggregate = inspector.report
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
      end
    end

  end
end