require "net/http"

connection = Net::HTTP.start('127.0.0.1', '8080')

20_000.times do |i|
  puts i if i % 1000 == 0
  connection.get '/?action=increase&key=_a'
end