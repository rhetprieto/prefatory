require 'redis'

module Prefatory
  module Storage
    class RedisProvider
      def initialize(
          options = nil,
          ttl = Prefatory.config.ttl,
          key_generator: Prefatory.config.keys.generator.new,
          marshaler: Prefatory.config.storage.marshaler,
          redis_client: Prefatory.config.storage.redis_client
      )
        options = default_settings(options)
        @ttl = ttl
        @key_generator = key_generator
        @marshaler = marshaler
        @client = redis_client
        @client ||= options ? Redis.new(options) : Redis.current
      end

      def set(key, value, ttl=nil)
        if is_pool?
          @client.with do |conn|
            conn.set(prefix(key), @marshaler.dump(value), ex: ttl||@ttl)
          end
        else
          @client.set(prefix(key), @marshaler.dump(value), ex: ttl||@ttl)
        end
      end

      def get(key)
        value = if is_pool?
                  @client.with do |conn|
                    conn.get(prefix(key))
                  end
                else
                  @client.get(prefix(key))
                end

        value ? @marshaler.load(value) : value
      end

      def delete(key)
        if is_pool?
          @client.with do |conn|
            conn.del(prefix(key))
          end
        else
          @client.del(prefix(key))
        end
      end

      def key?(key)
        if is_pool?
          @client.with do |conn|
            conn.exists?(prefix(key))
          end
        else
          @client.exists?(prefix(key))
        end
      end

      def next_key(obj=nil)
        @key_generator.key(obj)
      end

      private

      def prefix(key)
        @key_generator.prefix(key)
      end

      def default_settings(options)
        return options if options&.fetch(:url){false} || options&.fetch(:host){false}
        url = ENV['REDIS_PROVIDER'] || ENV['REDIS_URL'] || ENV['REDIS_SERVER']
        if (url)
          options ||= {}
          options = options.merge(url: url)
        end
        options
      end

      def is_pool?
        @is_pool ||= defined?(ConnectionPool) && @client.is_a?(ConnectionPool)
      end
    end
  end
end
