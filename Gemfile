source 'https://rubygems.org'

gemspec

group :development do
  gem 'rdoc'
  gem 'cassandra-driver', '~> 1.1', require: 'cassandra'
  gem 'activesupport', require: 'active_support/all'
  require 'active_support/core_ext/class/attribute_accessors'
end

group :test do
  gem 'rspec', '~> 3.1.0', require: false
  gem 'rspec-its'
  gem 'guard-rspec'
  gem 'guard-bundler'
  gem 'guard'
  gem 'pry'
  gem 'timecop'
  gem 'simplecov', require: false
end
