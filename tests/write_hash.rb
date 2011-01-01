#!/usr/bin/env ruby

require "rubygems"
require "net/http"
require "pp"
require "yajl"
require "yajl/json_gem"
require "benchmark"

$port = ARGV.shift || "8080"

DOMAIN = '127.0.0.1'


$connection = Net::HTTP.start(DOMAIN, $port)
puts "== connected to #{DOMAIN}:#{$port}"


def send_request uri, post = nil
  res = ""
  #puts "-- SENDING #{uri} | #{post}"
  $connection.post(uri, post) {|r| res += r }
  puts ">>> " + res
end

family = {
  :simpsons => {
    :father => {
      :_name  => "Homer",
      :_age   => 32
    },
    
    :mother => {
      :_name  => "Marge",
      :_age   => 35
    },
    
    :son => {
      :_name  => "Bart",
      :_age   => 8,
      :dreams  => {
        :_sex   => "any girl",
        :_drugs => "only light"
      }
    },
    
    :dauther => {
      :_name  => "Lisa",
      :_age   => 10
    }
  }
}

100.times do |i|
  send_request "/?action=write_hash&key=tt#{i}", JSON.generate(family)
end

dump = { :_object => Object.new, :methods => {} }
dump[:_object].methods.each do |m|
  dump[:methods]["_" + m] = dump[:_object].method(m).to_s
end

send_request "/?action=write_hash&key=object", JSON.generate(dump)

extra_large = {}
(0..1000).to_a.each do |num|
  extra_large["#{num}_value"] = {"methods" => dump[:methods], "family" => family}
end

stringified = JSON.generate(extra_large)

puts "loading #{stringified.size}-size string"
puts Benchmark.measure {
  send_request "/?action=write_hash&key=extra_large", JSON.generate(extra_large)
}