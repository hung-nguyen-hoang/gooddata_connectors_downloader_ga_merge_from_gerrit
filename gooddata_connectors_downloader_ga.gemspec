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

  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'rake'
  spec.add_dependency 'gooddata'
  spec.add_dependency 'google-api-client', '~> 0.9'
end
