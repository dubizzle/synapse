require_relative "./base"

require 'zk'

module Synapse
  class VersionWatcher < BaseWatcher
    def start
      zk_hosts = @discovery['hosts'].shuffle.join(',')

      log.info "synapse: starting ZK watcher #{@name} @ hosts: #{zk_hosts}, 
                services path: #{@discovery['path']}, 
                version_path: #{@discovery['version_path']}"
      @zk = ZK.new(zk_hosts)

      # call the callback to bootstrap the process
      version_watcher_callback.call
      service_watcher_callback.call
    end

    def ping?
      @zk.ping?
    end

    private
    def validate_discovery_opts
      raise ArgumentError, "invalid discovery method #{@discovery['method']}" \
        unless @discovery['method'] == 'version'
      raise ArgumentError, "missing or invalid zookeeper host for service #{@name}" \
        unless @discovery['hosts']
      raise ArgumentError, "invalid zookeeper path for service #{@name}" \
        unless @discovery['path']
      raise ArgumentError, "invalid zookeeper version path for service #{@name}" \
        unless @discovery['version_path']          
    end

    # helper method that ensures that the discovery path exists
    def create(path)
      log.debug "synapse: creating ZK path: #{path}"
      # recurse if the parent node does not exist
      create File.dirname(path) unless @zk.exists? File.dirname(path)
      @zk.create(path, ignore: :node_exists)
    end

    # find the current backends at the discovery path; sets @backends
    def service_discover
      log.info "synapse: discovering backends for service #{@name}"

      new_backends = []
      begin
        @zk.children(@discovery['path'], :watch => true).map do |name|
          node = @zk.get("#{@discovery['path']}/#{name}")

          begin
            host, port = deserialize_service_instance(node.first)
          rescue
            log.error "synapse: invalid data in ZK node #{name} at #{@discovery['path']}"
          else
            server_port = @server_port_override ? @server_port_override : port

            log.debug "synapse: discovered backend #{name} at #{host}:#{server_port} for service #{@name}"
            new_backends << { 'name' => name, 'host' => host, 'port' => server_port}
          end
        end
      rescue ZK::Exceptions::NoNode
        # the path must exist, otherwise watch callbacks will not work
        create(@discovery['path'])
        retry
      end

      if new_backends.empty?
        if @default_servers.empty?
          log.warn "synapse: no backends and no default servers for service #{@name}; using previous backends: #{@backends.inspect}"
        else
          log.warn "synapse: no backends for service #{@name}; using default servers: #{@default_servers.inspect}"
          @backends = @default_servers
        end
      else
        log.info "synapse: discovered #{new_backends.length} backends for service #{@name}"
        @backends = new_backends
      end
    end

    # find the current backends at the discovery path; sets @backends
    def version_discover
      log.info "synapse: discovering version for service #{@name}"

      begin
        version = @zk.get(@discovery['version_path'], :watch => true).first
        synapse_config = '/opt/smartstack/synapse/config.json'
        log.debug "synapse: discovered version #{version}"
        updated_path = @discovery['path'].split("/")
        # remove the old version
        updated_path.pop
        # append the new version
        updated_path = updated_path + ["#{version}"]
        updated_path = updated_path.join("/")
        log.info("updated path #{updated_path}")
        if @discovery['path'] != updated_path
          @restart_synapse = true
          File.open( synapse_config, "r" ) do |f|
            @config_data = JSON.load( f )
            log.info("updating path #{updated_path} for service #{name}")
            @config_data['services'][name]['discovery']['path'] = updated_path
          end
          File.open( synapse_config, "w" ) do |fw|
            fw.write(JSON.pretty_generate(@config_data))
          end
        end
      rescue ZK::Exceptions::NoNode
        # the path must exist, otherwise watch callbacks will not work
        create(@discovery['version_path'])
        retry
      end      
    end
    
    # sets up zookeeper callbacks if the data at the discovery path changes
    def service_watch
      @service_watcher.unsubscribe if defined? @service_watcher
      @service_watcher = @zk.register(@discovery['path'], &service_watcher_callback)
    end
    
    # sets up zookeeper callbacks if the data at the discovery path changes
    def version_watch
      @version_watcher.unsubscribe if defined? @version_watcher
      @version_watcher = @zk.register(@discovery['version_path'], &version_watcher_callback)
    end

    # handles the event that a watched path has changed in zookeeper
    def service_watcher_callback
      @service_callback ||= Proc.new do |event|
        # Set new watcher
        service_watch
        # Rediscover
        service_discover
        # send a message to calling class to reconfigure
        @synapse.reconfigure!
      end
    end
    
    # handles the event that a watched path has changed in zookeeper
    def version_watcher_callback
      @version_callback ||= Proc.new do |event|
        # Set new watcher
        version_watch
        # Rediscover
        version_discover
        # restart synapse
        if @restart_synapse
          @restart_synapse = false
          log.info("restarting synapse")
          res = `sudo service synapse reload`
          log.debug(res)
          raise "failed to reload haproxy via command}: #{res}" unless $?.success?
        end
      end
    end

    # tries to extract host/port from a json hash
    def parse_json(data)
      begin
        json = JSON.parse data
      rescue Object => o
        return false
      end
      raise 'instance json data does not have host key' unless json.has_key?('host')
      raise 'instance json data does not have port key' unless json.has_key?('port')
      return json['host'], json['port']
    end

    # decode the data at a zookeeper endpoint
    def deserialize_service_instance(data)
      log.debug "synapse: deserializing process data"

      # if that does not work, try json
      host, port = parse_json(data)
      return host, port if host

      # if we got this far, then we have a problem
      raise "could not decode this data:\n#{data}"
    end
  end
end
