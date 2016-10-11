module GoodData
  module Connectors
    module GoogleAnalyticsDownloader
      class DownloaderGoogleAnalytics < Base::BaseDownloader
        attr_accessor :ga

        require 'google/apis/analyticsreporting_v4'

        GA = Google::Apis::AnalyticsreportingV4
        TYPE = 'ga'.freeze
        CUSTOM_FIELDS = %w(segment filter profile).freeze

        def initialize(metadata, options = {})
          $now = GoodData::Connectors::Metadata::Runtime.now
          super(metadata, options)
        end

        def connect
          $log.info 'Connecting to Google Analytics'

          options = {}
          options[:refresh_token] = @metadata.get_configuration_by_type_and_key(TYPE, 'refresh_token')
          options[:client_id] = @metadata.get_configuration_by_type_and_key(TYPE, 'client_id')
          options[:client_secret] = @metadata.get_configuration_by_type_and_key(TYPE, 'client_secret')

          self.ga = log_in(options)
        end

        def get_dimensions(entity)
          dimensions = entity.custom['dimensions']
          return nil unless dimensions
          dimensions_arr = []
          dimensions.split(',').each do |dimension|
            dimensions_arr << GA::Dimension.new(name: dimension)
          end
          dimensions_arr
        end

        def get_segments(line)
          segment = line['segment']
          return nil unless segment
          [GA::Segment.new(segment_id: segment)]
        end

        def get_metrics(entity)
          metrics = entity.custom['metrics']
          raise 'Metrics are missing in entity configuration' unless metrics
          metrics_arr = []
          metrics.split(',').each do |metric|
            metrics_arr << GA::Metric.new(expression: metric)
          end
          metrics_arr
        end

        def get_filters(line)
          filters = line['filters']
          return nil unless filters
          filters.empty? ? nil : filters
        end

        def get_report(entity, line, start_date, end_date)
          profile_id = line['profile_id']
          raise 'Missing profile id' unless profile_id

          date_ranges = [GA::DateRange.new(start_date: start_date, end_date: end_date)]
          report_request = GA::ReportRequest.new(
            dimensions: get_dimensions(entity),
            date_ranges: date_ranges,
            segments: get_segments(line),
            filters_expression: get_filters(line),
            sampling_level: 'LARGE',
            metrics: get_metrics(entity),
            page_size: 10_000,
            view_id: line['profile_id']
          )
          report_requests = GA::GetReportsRequest.new(report_requests: [report_request])
          ga.batch_get_reports(report_requests).reports.first
        end

        def save_report_data(entity, report, start_date, line)
          local_path = "output/#{entity.name}_#{start_date.to_i}.csv"
          CSV.open(local_path, 'w', col_sep: ',') do |csv|
            csv << get_headers(report)
            report.data.rows.each do |row|
              csv << row.dimensions + row.metrics.first.values + [line['segment'], line['filters'], line['profile_id']]
            end
          end
          local_path
        end

        def download_data
          $log.info 'Processing queries info file'
          csv = load_queries_csv
          $log.info 'Downloading data from Google Analytics'
          csv.each do |line|
            entity = @metadata.get_entity(line['entity'])
            next unless entity
            full = @metadata.get_configuration_by_type_and_key(TYPE, 'full')
            start_date = entity.previous_runtime.empty? || !full ? (DateTime.now - 14.days) : DateTime.parse(line['initial_load_start_date'])
            end_date = DateTime.now
            $log.info "Downloading entity #{entity.name}"
            report = get_report(entity, line, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            $log.info "Processing entity #{entity.name}"
            local_path = save_report_data(entity, report, start_date, line)
            load_metadata(entity, report)
            save_data(entity, local_path, start_date, end_date) if local_path
          end
        end

        def validation_schema
          File.join(File.dirname(__FILE__), 'schema/validation_schema.json')
        end

        def define_default_entities
          []
        end

        private

        def load_queries_csv
          queries_path = @metadata.get_configuration_by_type_and_key(TYPE, 'queries_info_file_path')
          raise 'You need to provide path to queries source file' unless queries_path
          file = queries_path.split('/').last
          local_path = "tmp/#{file}"
          @metadata.download_data(queries_path, local_path)
          parsed_file = File.open(local_path, 'r:bom|utf-8').read.delete("'")
          CSV.parse(parsed_file, headers: true, header_converters: ->(h) { h.try(:downcase) }, row_sep: :auto, col_sep: ',')
        end

        def save_data(metadata_entity, local_path, start_date, end_date)
          $log.info 'Saving data to S3'
          local_path = pack_data(local_path)
          metadata_entity.store_runtime_param('source_filename', local_path)
          metadata_entity.store_runtime_param('date_to', end_date)
          metadata_entity.store_runtime_param('date_from', start_date)
          @metadata.save_data(metadata_entity)
          File.delete(local_path)
        end

        def log_in(options)
          ga = GA::AnalyticsReportingService.new
          ga.authorization = Signet::OAuth2::Client.new(
            token_credential_uri: 'https://www.googleapis.com/oauth2/v3/token',
            client_id: options[:client_id],
            client_secret: options[:client_secret],
            refresh_token: options[:refresh_token],
            grant_type: 'refresh_token'
          )
          ga.authorization.fetch_access_token!
          ga
        end

        def get_headers(report)
          headers = report.column_header.dimensions
          headers += report.column_header.metric_header.metric_header_entries.map(&:name)
          headers + CUSTOM_FIELDS
        end

        def load_metadata(entity, report)
          return nil unless entity.disabled?
          load_entity_fields(entity, report)
          load_entity_custom_metadata(entity)
          metadata.save_entity(entity)
        end

        def load_entity_fields(entity, report)
          temporary_fields = process_fields(report)
          diff = entity.diff_fields(temporary_fields)
          load_fields_from_source(diff, entity)
          disable_fields(diff, entity)
          change_fields(diff, entity)
        end

        def process_fields(report)
          fields = []
          report.column_header.dimensions.each do |header|
            fields << new_field(header, header == 'ga:date' ? 'date-false' : 'string-255')
          end
          report.column_header.metric_header.metric_header_entries.each do |header|
            fields << new_field(header.name, get_field_type(header.type))
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
            return 'string-255'
          end
        end

        def new_field(name, type)
          Metadata::Field.new('id' => name,
                              'name' => name,
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
