require 'mutex_m'
require 'set'

module Rack
  # Tries to discover any potential thread-safety issues and prints them to $stdout.
  #
  # Usage in Rails:
  #   config.middleware.use 'Rack::ThreadSafetyTester', :whitelist => {
  #     Hash => ['active_support/dependencies.rb'], # dev. mode related
  #     Set => 'active_support/dependencies.rb', # dev. mode related
  #     Array => ['active_support/dependencies.rb'] # dev. mode related
  #   },
  #   :skip_paths => [/\A\/assets\//] # skip /assets to keep things snappy
  #
  #   # or best inserted at the top of the middleware chain:
  #   config.middleware.insert_before 'ActionDispatch::Static', 'Rack::ThreadSafetyTester'
  class ThreadSafetyTester
    class DataStructureWatcher
      OFFENDERS = {}

      module Agent
        private
        def attempting_modification!(klass_name)
          if $thread_safety_testing
            backtrace = caller
            backtrace.shift # shift `attempting_modification!` out of the backtrace
            OFFENDERS[klass_name][backtrace.first] ||= [self, backtrace]
          end
        end
      end

      attr_reader :klass

      def initialize(klass, methods, whitelist = [])
        @offenders = OFFENDERS[klass.name] = {}
        @klass     = klass
        @whitelist = Set.new(whitelist)
        define_agent(klass.name, methods)
      end

      def inject!
        ObjectSpace.each_object(@klass) do |instance|
          instance.extend(@agent_module) unless instance.frozen? || internal_data_structure?(instance) || instance.singleton_class.include?(@agent_module)
        end
      end

      def report_offenders!
        @file_whitelist = nil
        @offenders.each do |culprit, (inst, backtrace)|
          puts "[THREAD_SAFETY] Potential concurrent #{@klass.name} modification at: #{culprit}" unless ignore?(culprit, inst) || yield(inst)
        end
        @offenders.clear
      end

      def whitelist!(*args)
        @whitelist.merge(args.flatten.compact)
      end

      private
      def define_agent(klass_name, methods)
        @agent_module = Module.new do
          include Agent
          methods.each do |method|
            class_eval <<-RUBY_EVAL, __FILE__, __LINE__
              def #{method}(*args, &block)
                attempting_modification!(#{klass_name.inspect})
                super
              end
            RUBY_EVAL
          end
        end
      end

      def ignore?(culprit, inst)
        internal_data_structure?(inst) || file_whitelist.any? {|file| culprit.include?(file)}
      end

      def file_whitelist
        # is there a better way?
        @file_whitelist ||= begin
          arr = []
          @whitelist.each {|fnmatch_rule| arr.concat(to_loaded_paths(fnmatch_rule))}
          arr
        end
      end

      def to_loaded_paths(fnmatch_rule)
        arr = []
        $LOAD_PATH.each do |path|
          matcher = ::File.join(path, fnmatch_rule)
          $LOADED_FEATURES.each do |loaded_feature|
            arr << loaded_feature if ::File.fnmatch?(matcher, loaded_feature)
          end
        end
        arr
      end

      def internal_data_structure?(obj)
      end
    end

    class HashWatcher < DataStructureWatcher
      private
      def internal_data_structure?(obj)
        OFFENDERS.values.any? {|hash| hash.equal?(obj)}
      end
    end

    HASH_WATCHER = HashWatcher.new(Hash,
                                   %w([]= clear delete delete_if fetch keep_if merge! reject! replace select! shift store update),
                                   %w(set.rb thread_safe/*.rb))
    ARRAY_WATCHER = DataStructureWatcher.new(Array,
                                             %w([]= clear collect! compact! concat delete delete_at delete_if drop drop_while fetch fill flatten! insert
                                                keep_if map! pop push reject! replace reverse! rotate! select! shift shuffle! slice! sort! sort_by!
                                                uniq! unshift),
                                             %w(thread.rb))
    SET_WATCHER = DataStructureWatcher.new(Set,
                                           %w(<< add add? clear collect! delete delete? delete_if flatten! keep_if map! merge reject! replace select! subtract))

    WATCHERS = [HASH_WATCHER, ARRAY_WATCHER, SET_WATCHER]

    class Tester
      def initialize(ignorables = [])
        @ignorables = ignorables
      end

      def watch
        setup!
        yield
      ensure
        report!
      end

      def setup!
        GC.start
        WATCHERS.each {|watcher| watcher.inject!}
        $thread_safety_testing = true
      end

      def report!
        $thread_safety_testing = false
        WATCHERS.each do |watcher|
          watcher.report_offenders! {|offender| ignorable?(offender)}
        end
      end

      private
      def ignorable?(obj)
        @ignorables.any? {|ignorable| ignorable.equal?(obj)}
      end
    end

    include Mutex_m

    def initialize(app, options = {})
      super()
      if options.kind_of?(Hash)
        if (whitelists = options[:whitelist]).kind_of?(Hash)
          WATCHERS.each do |watcher|
            if whitelist = whitelists[watcher.klass]
              watcher.whitelist!(whitelist)
            end
          end
        end

        if (skip_paths = options[:skip_paths]).present?
          @skip_paths = Array(skip_paths)
        end
      end
      @app = app
    end

    def call(env)
      if @skip_paths && @skip_paths.any? {|skip_path| skip_path === env['PATH_INFO']}
        @app.call(env)
      else
        watch_usage!(env) do
          status, headers, body = @app.call(env)

          # apparently this is how a proper rack middleware is supposed to look if one wants to avoid thread deadlocks while intercepting chunked responses
          if body && body.respond_to?(:each)
            new_body = []
            body.each {|part| new_body << part}
            body.close if body.respond_to?(:close)
            body = new_body
          end

          [status, headers, body]
        end
      end
    end

    private
    def watch_usage!(env, &block)
      synchronize do # we ourselves are not thread-safe
        Tester.new([env]).watch(&block)
      end
    end
  end
end