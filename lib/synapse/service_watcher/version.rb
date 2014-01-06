require_relative "zookeeper"

require 'zk'

module Synapse
  class VersionWatcher < ZookeeperWatcher
    def start
      zk_hosts = @discovery['hosts'].shuffle.join(',')

      log.info "synapse: starting version watcher #{@name} @ hosts: #{zk_hosts}, 
                services path: #{@discovery['path']}, 
                version_path: #{@discovery['version_path']}"
      @zk = ZK.new(zk_hosts)

      # call the callback to bootstrap the process
      # starting ZK watcher for nodes lookup
      watcher_callback.call
      # starting active production version watcher
      version_watcher_callback.call
    end

    private
    def validate_discovery_opts
      raise ArgumentError, "invalid discovery method #{@discovery['method']}" \
        unless @discovery['method'] == 'version'
      %w{hosts path version_path synapse_config reload_command}.each do |required|
        raise ArgumentError, "missing required argument #{required} in VersionWatcher check" \
          unless  @discovery[required]
      end
    end

    # find the current version at the discovery path; update synapse config
    def version_discover
      log.info "synapse: discovering version for service #{@name}"

      begin
        version = @zk.get(@discovery['version_path'], :watch => true).first
        synapse_config = @discovery['synapse_config']
        log.debug "synapse: discovered version #{version}"
        updated_path = @discovery['path'].split("/")
        # remove the old version
        updated_path.pop
        # append the new version
        updated_path = updated_path + ["v#{version}"]
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
    def version_watch
      @version_watcher.unsubscribe if defined? @version_watcher
      @version_watcher = @zk.register(@discovery['version_path'], &version_watcher_callback)
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
          res = `#{@discovery['reload_command']}`
          log.debug(res)
          raise "failed to reload haproxy via #{@discovery['reload_command']}: #{res}" unless $?.success?
        end
      end
    end

  end
end
