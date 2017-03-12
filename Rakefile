require 'bundler/gem_tasks'


require 'rake/testtask'
require 'rake/notes/rake_task'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'

task :cop do
  exec 'rubocop -a -c .rubocop.yml lib/ spec/'
end

RSpec::Core::RakeTask.new(:test)

namespace :test do
  desc 'Run unit tests'
  RSpec::Core::RakeTask.new(:unit) do |t|
    t.pattern = 'spec/unit/**/*.rb'
  end

  # desc 'Run integration tests'
  # RSpec::Core::RakeTask.new(:integration) do |t|
  #   t.pattern = 'spec/features/*.rb'
  # end

  # task all: [:unit, :integrtion]
  task all: [:unit]
end
