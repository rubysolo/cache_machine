class CacheMachine
  module Response # :nodoc:
    STORED      = "STORED\r\n"
    NOT_STORED  = "NOT_STORED\r\n"
    EXISTS      = "EXISTS\r\n"
    NOT_FOUND   = "NOT_FOUND\r\n"
    DELETED     = "DELETED\r\n"
  end

  class << self
    def read(key, options={})
      operation('read', options[:quiet], nil) { connection.get(key, options[:raw]) }
    end

    def write(key, value, options={})
      operation('write', options[:quiet]) do
        method = options[:unless_exist] ? :add : :set
        value = value.to_s if options[:raw]
        response = connection.send(method, key, value, expiration(options), options[:raw])
        response == Response::STORED
      end
    end

    def fetch(key, options={})
      if ! options[:force] && value = read(key, options.merge(:quiet => true))
        log "hit: #{log_key_and_options(key, options)}"
        value
      elsif block_given?
        log "miss: #{log_key_and_options(key, options)}"

        value = nil
        ms = Benchmark.ms { value = yield }

        write(key, value, options.merge(:quiet => true))
        log "write (will save %.2fms): #{log_key_and_options(key, options)}" % ms

        value
      end
    end

    def delete(key, options={})
      operation('delete', options[:quiet]) do
        response = connection.delete(key, expiration(options))
        response == Response::DELETED
      end
    end

    def clear
      connection.flush_all
    end

    def method_missing(method_id, *args)
      connection.send(method_id, *args)
    end

    private

    def operation(name, quiet=false, error_response=false, &block)
      log name unless quiet
      yield
    rescue ArgumentError => e
      autoloaded_classes ||= Hash.new { |hash, key| hash[key] = true; false }
      retry if e.to_s[/undefined class|referred/] && ! autoloaded_classes[e.to_s.split.last.constantize]
      raise e
    rescue MemCache::MemCacheError => e
      log "#{name} : MemCacheError (#{e}): #{e.message}"
      error_response
    end

    def connection
      @connection ||= begin
        # => [:mem_cache_store, "localhost:11211", {:namespace=>"friend_interview"}]
        _, address, options = Rails.configuration.cache_store
        MemCache.new(address, options)
      end
    end

    def expiration(options={})
      returning(options[:expires_in] || 0) do |expires_in|
        raise ":expires_in must be a number" unless expires_in.is_a?(Numeric)
      end
    end

    def log(msg)
      RAILS_DEFAULT_LOGGER.info "[CacheMachine] #{msg}"
    end

    def log_key_and_options(key, options)
      returning key.dup do |msg|
        msg << " (#{options.inspect})" unless options.empty?
      end
    end
  end
end