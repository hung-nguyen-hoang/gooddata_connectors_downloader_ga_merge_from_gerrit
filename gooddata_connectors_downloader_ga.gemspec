# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_downloader_ga/version'

Gem::Specification.new do |spec|
  spec.name          = 'gooddata_connectors_downloader_ga'
  spec.version       = GoodData::Connectors::GoogleAnalyticsDownloader::VERSION
  spec.authors       = ['Jan Kreuzzieger']
  spec.email         = ['jan.kreuzzieger@gooddata.com']
  spec.description   = 'The gem wraping the google analytics connector implementation for Gooddata Connectors infrastructure'
  spec.summary       = ''
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($INPUT_RECORD_SEPARATOR)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake', '~> 10.4', '>= 10.4.2'
  spec.add_development_dependency 'rake-notes', '~> 0.2', '>= 0.2.0'
  spec.add_development_dependency 'rspec', '~> 3.3', '>= 3.3.0'
  spec.add_development_dependency 'rubocop', '~> 0.41.2'
  spec.add_development_dependency 'simplecov', '~> 0.10', '>= 0.10.0'
  spec.add_dependency 'activesupport', '~> 4.1', '>= 4.1.0'
  spec.add_dependency 'gooddata', '~> 0.6', '= 0.6.50'
  spec.add_dependency 'gooddata_datawarehouse'
end
