module GoodData
  module Connectors
    module GoogleAnalyticsDownloader
      class DownloaderGoogleAnalytics < Base::BaseDownloader
        attr_accessor :ga, :client

        require 'active_support/all'
        require 'google/api_client'

        TYPE = 'ga'.freeze
        CUSTOM_FIELDS = %w(segment filter profile).freeze

        def initialize(metadata, options = {})
          $now = GoodData::Connectors::Metadata::Runtime.now
          super(metadata, options)
        end

        def connect
          $log.info 'Connecting to Google Analytics'

          self.client = Google::APIClient.new
          self.ga = client.discovered_api('analyticsreporting', 'v4')

          options = {}
          options[:refresh_token] = @metadata.get_configuration_by_type_and_key(TYPE, 'refresh_token')
          options[:client_id] = @metadata.get_configuration_by_type_and_key(TYPE, 'client_id')
          options[:client_secret] = @metadata.get_configuration_by_type_and_key(TYPE, 'client_secret')

          log_in(options)
        end

        def download_data
          $log.info 'Processing queries info file'
          csv = load_queries_csv

          $log.info 'Downloading data from Google Analytics'
          entities_data = {}

          csv.each do |line|
            entities_data[line['entity']] = [] unless entities_data[line['entity']]
            entities_data[line['entity']] << line
          end

          entities_data.each do |entity_data|
            next unless entity = @metadata.get_entity(entity_data[0])
            process_entity_data(entity, entity_data[1])
          end
          create_profile_entity
        end

        def process_entity_data(entity, entity_data)
          rolling_days = @metadata.get_configuration_by_type_and_key(TYPE, 'rolling_days') || 14
          end_date = DateTime.now
          local_path = nil
          loaded_metadata = false

          $log.info "Downloading entity #{entity.name}"
          entity_data.each do |line|
            $log.info "Downloading profile #{line['profile_id']}"
            start_date = get_start_date(entity, line, rolling_days)
            $log.info "Downloading data from #{start_date.strftime('%Y-%m-%d')} to #{end_date.strftime('%Y-%m-%d')}"
            report = get_report(entity, line, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            next unless report && report['data']['rows']

            loaded_metadata = load_metadata(entity, report) unless loaded_metadata

            if local_path
              append_report_data(local_path, report, line)
            else
              local_path = create_report_data(entity, report, line)
            end

            while next_page_token = report['nextPageToken']
              $log.info "Downloaded #{report['nextPageToken']} rows"
              next_report = get_report(entity, line, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), next_page_token)
              if next_report
                report = next_report
                append_report_data(local_path, report, line)
              else
                report['nextPageToken'] = nil
              end
            end
          end

          $log.info "Processing entity #{entity.name}"
          save_data(entity, local_path, end_date) if local_path
          $log.info "Entity #{entity.name} processed"
        end

        def validation_schema
          File.join(File.dirname(__FILE__), 'schema/validation_schema.json')
        end

        def define_default_entities
          []
        end

        private

        def create_profile_entity
          entity = metadata.list_entities.select{|entity| entity.custom && entity.custom['type'] == 'ga_profile'}.first
          return nil unless entity

          analytics = client.discovered_api('analytics', 'v3')
          result = client.execute(
            :api_method => analytics.management.profiles.list,
            parameters: {accountId: '~all',webPropertyId: '~all'}
          )

          items = result.data.items
          raise 'You have insufficient privileges or user does not have any Google Analytics accounts' if items.empty?
          keys = items.first.to_hash.select{|k,v| v.class != Hash}.keys
          entity = new_profile_entity(entity, keys)
          local_path = "output/profile_#{Time.now.to_i}.csv"
          CSV.open(local_path, 'w', col_sep: ',') do |csv|
            csv << keys
            result.data.items.each do |row|
              csv << keys.map{|key| key=="id" ? "ga:#{row.to_hash[key]}" : row.to_hash[key]}
            end if result.data.items
          end
          metadata.entities << entity
          load_metadata(entity)
          save_data(entity, local_path)
        end

        def new_profile_entity(entity, keys)
          fields = []
          keys.each do |name|
            fields << new_field(name, 'string-255') # maybe less?
          end
          entity.custom['full'] = true
          load_entity_fields(entity, nil, fields)
          entity
        end

        def get_dimensions(entity)
          dimensions = entity.custom['dimensions']
          return nil unless dimensions
          dimensions_arr = []
          dimensions.split(',').each do |dimension|
            dimensions_arr << {'name' => dimension}
          end
          dimensions_arr
        end

        def get_segments(line)
          segment = line['segment']
          return nil unless segment
          [{'segmentId' => segment}]
        end

        def get_metrics(entity)
          metrics = entity.custom['metrics']
          raise 'Metrics are missing in entity configuration' unless metrics
          metrics_arr = []
          metrics.split(',').each do |metric|
            metrics_arr << {'expression' => metric}
          end
          metrics_arr
        end

        def get_filters(line)
          filters = line['filters']
          return nil unless filters
          filters.empty? ? nil : filters
        end

        def get_report(entity, line, start_date, end_date, next_page_token = nil)
          profile_id = line['profile_id']
          raise 'Missing profile id' unless profile_id

          date_ranges = [{"startDate" => start_date, "endDate" => end_date}]
          parameters = {
            'reportRequests'=> [
            {
              'viewId' => line['profile_id'],
              'pageSize' => 10_000,
              'samplingLevel' => 'LARGE',
              'dateRanges' => date_ranges,
              'metrics' => get_metrics(entity),
              'filtersExpression' => get_filters(line),
              'segments' => get_segments(line),
              'dimensions' => get_dimensions(entity),
              'pageToken' => next_page_token
            }]
          }

          response = send_report_request(parameters)
          data = JSON.parse(response.body)
          data['reports']  ? data['reports'].first : nil
        end

        def send_report_request(parameters)
          uri = URI.parse("https://analyticsreporting.googleapis.com")
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = true
          http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          request = Net::HTTP::Post.new("/v4/reports:batchGet")
          request.add_field('Content-Type', 'application/json')
          request.add_field('Authorization', "Bearer #{client.authorization.access_token}")
          request.body = parameters.to_json
          http.request(request)
        end

        def get_start_date(entity, line, rolling_days)
          # Save information about previous load per entity per profile id
          full = @metadata.get_configuration_by_type_and_key(TYPE, 'full')
          full |= entity.custom['full'].class == Array && (entity.custom['full'].include?['all'] || entity.custom['full'].include?[line['profile_id']])
          cache = metadata.load_cache('previous_runtimes')
          cache.hash[entity.id] = {} unless cache.hash[entity.id]
          previous_runtime = cache.hash[entity.id][line['profile_id']]
          start_date = previous_runtime.nil? || full ? DateTime.parse(line['initial_load_start_date']) : (DateTime.now - rolling_days.to_i.days)
          cache.hash[entity.id][line['profile_id']] = start_date
          metadata.save_cache(cache.id)
          start_date
        end

        def create_report_data(entity, report, line)
          local_path = "output/#{entity.name}_#{Time.now.to_i}.csv"
          CSV.open(local_path, 'w', col_sep: ',') do |csv|
            csv << get_headers(report)
            report['data']['rows'].each do |row|
              csv << row['dimensions'] + row['metrics'].first['values'] + [line['segment'], line['filters'], line['profile_id']]
            end if report
          end
          local_path
        end

        def append_report_data(local_path, report, line)
          CSV.open(local_path, 'a', col_sep: ',') do |csv|
            report['data']['rows'].each do |row|
              csv << row['dimensions'] + row['metrics'].first['values'] + [line['segment'], line['filters'], line['profile_id']]
            end if report
          end
          local_path
        end

        def load_queries_csv
          queries_path = @metadata.get_configuration_by_type_and_key(TYPE, 'queries_info_file_path')
          raise 'You need to provide path to queries source file' unless queries_path
          file = queries_path.split('/').last
          local_path = "tmp/#{file}"
          @metadata.download_data(queries_path, local_path)
          parsed_file = File.open(local_path, 'r:bom|utf-8').read.delete("'")
          CSV.parse(parsed_file, headers: true, header_converters: ->(h) { h.downcase }, row_sep: :auto, col_sep: ',')
        end

        def save_data(metadata_entity, local_path, end_date = Time.now)
          $log.info 'Saving data to S3'
          local_path = pack_data(local_path)
          metadata_entity.store_runtime_param('source_filename', local_path)
          metadata_entity.store_runtime_param('date_to', end_date)
          metadata_entity.store_runtime_param('date_from', Time.now)
          @metadata.save_data(metadata_entity)
          File.delete(local_path)
        end

        def log_in(options)

          # ga = GA::AnalyticsReportingService.new
          client.authorization = Signet::OAuth2::Client.new(
            token_credential_uri: 'https://www.googleapis.com/oauth2/v3/token',
            client_id: options[:client_id],
            client_secret: options[:client_secret],
            refresh_token: options[:refresh_token],
            grant_type: 'refresh_token'
          )
          client.authorization.fetch_access_token!
        end

        def get_headers(report)
          headers = report['columnHeader']['dimensions']
          headers += report['columnHeader']['metricHeader']['metricHeaderEntries'].map{|entry| entry['name']}
          headers + CUSTOM_FIELDS
        end

        def load_metadata(entity, report = nil)
          return nil if entity.disabled?
          load_entity_fields(entity, report) if report
          load_entity_custom_metadata(entity)
          metadata.save_entity(entity)
          true
        end

        def load_entity_fields(entity, report = nil, fields = nil)
          temporary_fields = report ? process_fields(report) : fields
          diff = entity.diff_fields(temporary_fields)
          load_fields_from_source(diff, entity)
          disable_fields(diff, entity)
          change_fields(diff, entity)
        end

        def process_fields(report)
          fields = []
          report['columnHeader']['dimensions'].each do |header|
            fields << new_field(header, header == 'ga:date' ? 'date-false' : 'string-1024')
          end
          report['columnHeader']['metricHeader']['metricHeaderEntries'].each do |header|
            fields << new_field(header['name'], get_field_type(header['type']))
          end
          CUSTOM_FIELDS.each do |header|
            fields << new_field(header, 'string-255') # maybe less?
          end
          fields
        end

        def get_field_type(type)
          case type
          when 'INTEGER'
            return 'integer'
          when 'TIME'
            return 'decimal-30-15'
          when 'CURRENCY'
            return 'decimal-30-15'
          when 'FLOAT'
            return 'decimal-30-15'
          when 'PERCENT'
            return 'decimal-30-15'
          else
            return 'string-1024'
          end
        end

        def new_field(name, type)
          Metadata::Field.new('id' => name.split(':').last,
                              'name' => name.split(':').last,
                              'type' => type,
                              'custom' => {})
        end

        def load_fields_from_source(diff, metadata_entity)
          diff['only_in_target'].each do |target_field|
            $log.info "Adding new field #{target_field.name} to entity #{metadata_entity.id}"
            target_field.order = metadata_entity.get_new_order_id
            metadata_entity.add_field(target_field)
            metadata_entity.make_dirty
          end
        end

        def disable_fields(diff, metadata_entity)
          diff['only_in_source'].each do |source_field|
            next if source_field.disabled?
            $log.info "Disabling field #{source_field.name} in entity #{metadata_entity.id}"
            source_field.disable('From synchronization with source system')
            metadata_entity.make_dirty
          end
        end

        def change_fields(diff, metadata_entity)
          diff['changed'].each do |change|
            source_field = change['source_field']
            $log.info "The field #{source_field.name} in entity #{metadata_entity.id} has changed"
            source_field.name = change['target_field'].name if change.include?('name')
            source_field.type = change['target_field'].type if change.include?('type')
            source_field.enabled = change['target_field'].enabled if change.include?('disabled')
            metadata_entity.make_dirty
          end
        end

        def load_entity_custom_metadata(metadata_entity)
          if !metadata_entity.custom.include?('download_by') || metadata_entity.custom['download_by'] != TYPE
            metadata_entity.custom['download_by'] = TYPE
            metadata_entity.make_dirty
          end

          if !metadata_entity.custom.include?('escape_as') || metadata_entity.custom['escape_as'] != '"'
            metadata_entity.custom['escape_as'] = '"'
            metadata_entity.make_dirty
          end

          if !metadata_entity.custom.include?('file_format') || metadata_entity.custom['file_format'] != 'GZIP'
            metadata_entity.custom['file_format'] = 'GZIP'
            metadata_entity.make_dirty
          end

          # Remove "ga:" part, we do not want that
          metadata_entity.custom['hub'] = metadata_entity.custom['hub'].map{|key| key.split(':').last}
          metadata_entity.store_runtime_param('full', true) if metadata_entity.custom['full']
        end

        def pack_data(input_filename)
          gzip = input_filename + '.gz'
          `gzip #{input_filename}`
          gzip
        end
      end
    end
  end
end
