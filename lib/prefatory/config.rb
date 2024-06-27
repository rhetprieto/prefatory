require 'logger'
require 'dry-configurable'
require_relative 'key_generator'

module Prefatory
  extend Dry::Configurable

  setting :logger, Logger.new(STDOUT)
  setting :ttl, 86400 # in seconds - defaults to one day
  setting :keys do
    setting :generator, Prefatory::KeyGenerator
    setting :prefix, 'prefatory'
    # If the object being stored responds to :entity_primary_key and that value is not null,
    # it is assumed to be a universally unique primary key for the object.
    # The default key_generator will use it
    setting :primary_uuid, :primary_uuid
  end
  setting :storage do
    setting :redis_client
    setting :provider # :memcached or :redis
    setting :options
    setting :marshaler, Marshal
  end
end
