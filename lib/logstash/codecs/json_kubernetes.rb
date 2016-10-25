# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "logstash/json"

class LogStash::Codecs::JsonKubernetes < LogStash::Codecs::Base

  # The codec name
  config_name "json_kubernetes"

  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"
  config :delimiter, :validate => :string, :default => "\n"
  config :source, :validate => :string, :default => "log"

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end # def register

  def decode(data)
    lines = @converter.convert(data).split(@delimiter).map do |line|
      decoded = LogStash::Json.load(line)
      case decoded
      when Hash
        decoded[@source]
      else
        ""
      end
    end
    yield LogStash::Event.new("message" => lines.join)
  rescue StandardError => e
    @logger.warn(
      "An unexpected error occurred parsing JSON data",
      :data => data,
      :message => e.message,
      :class => e.class.name,
      :backtrace => e.backtrace
    )
  end # def decode

  def encode_sync(event)
    if event.is_a?(LogStash::Event) and @format
      @on_event.call(event, event.sprintf(@format))
    else
      @on_event.call(event, event.to_s)
    end
  end # def encode_sync

end # class LogStash::Codecs::JsonKubernetes
