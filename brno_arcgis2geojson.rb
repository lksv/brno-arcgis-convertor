#!/usr/bin/ruby


require 'pp'


require 'faraday'
require 'faraday_middleware'
require 'active_record'

class Downloader
  BASE_BRNO_GIS_URL = 'http://gis.brno.cz/arcgis/rest/services/PUBLIC'
  DOWNLOAD_SLICE = 100

  attr_reader :map, :layer_index, :conn
  def initialize(map, layer_index, fields = '*')
    @map = map
    @layer_index = layer_index
    @fields = fields

    @conn = Faraday.new(:url => BASE_BRNO_GIS_URL) do |faraday|
      faraday.request  :url_encoded             # form-encode POST params
      faraday.response :logger                  # log requests to STDOUT
      faraday.request :json

      #althoug respons is json, Brno's server returns text/plain
      faraday.response :json #, :content_type => /\bjson$/

      faraday.adapter  Faraday.default_adapter  # make requests with Net::HTTP
    end
  end

  def url_path
    "#{map}/MapServer/#{layer_index}/query"
  end

  def oids
    return @oids if defined? @oids

    response = @conn.get url_path, {
      where: 'OBJECTID IS NOT NULL',
      returnIdsOnly: true,
      f: 'pjson'
    }
    @oids = response.body['objectIds']
  end

  def features
    return @features if defined? @features
    @features = []
    oids.each_slice(DOWNLOAD_SLICE) do |ids_bulk|
      response = @conn.get url_path, {
        where: "OBJECTID IN (#{ids_bulk.join(',')})",
        outFields: @fields,
        returnGeometry: true,
        f: 'pjson'
      }
      @features.concat(response.body['features'])
    end
    @features
  end

  def to_geojson(&block)
    f = features.map do |feature|
      geometry = convert_geometry(feature['geometry'])
      {
        type: "Feature",
        geometry: geometry,
        properties: block.call(feature['attributes']),
        id: feature['attributes']['OBJECTID']
      }
    end

    {
      type: "FeatureCollection",
      features: f
    }
  end

  private

  def convert_geometry(geometry)
    if geometry.keys.sort == ['x', 'y']
      return esri5514to4321({
        type: 'Point',
        coordinates: [geometry['x'], geometry['y']]
      })
    end
    geometry_type = geometry.keys.first
    geojson_type = case geometry_type
      when 'paths' then 'MultiLineString'
      else raise "Unknown type for geometry: #{geometry.inspect}"
    end
    geojson = {
      type: geojson_type,
      coordinates: geometry[geometry_type]
    }

    esri5514to4321(geojson)
  end

  def esri5514to4321(geojson)
    JSON.load(DB_CON.exec_query("SELECT ST_AsGeoJSON(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('#{JSON.dump(geojson)}'), 5514), 4326)) as geometry").to_hash.first['geometry'])
  end

end

ActiveRecord::Base.default_timezone = :utc
#ActiveRecord::Base.logger = Logger.new('log/development.db.log')
config = {
  adapter: "postgresql",
  encoding: "utf8",
  database: "ruian",
  pool: 10,
  username: "ob",
  password: "ob",
  host: "localhost"
}
ActiveRecord::Base.configurations = { 'developmnet' => config }
ActiveRecord::Base.establish_connection(:developmnet)

DB_CON = ActiveRecord::Base.connection

#p DB_CON.exec_query("SELECT 1 as res").to_hash
#geojson = {:type=>"MultiLineString", :coordinates=>[[[-598325.000015039, -1159973.0000053197], [-598305.0000150204, -1160007.0000053532], [-598286.3600150011, -1160041.5899573825], [-598288.6199830063, -1160049.7900213897], [-598272.17851099, -1160082.6563254222]]]}
#p JSON.load(DB_CON.exec_query("SELECT ST_AsGeoJSON(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON('#{JSON.dump(geojson)}'), 5514), 4326)) as geometry").to_hash.first['geometry'])
#__END__

#=begin

d = Downloader.new('uzavirky', 0)
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s: %s<br> %s<br> %s - %s" % [
       attributes['CISLO_JEDNACI'],
       attributes['NAZEV_AKCE'],
       attributes['POPIS'],
       attributes["POCATEK_UZAVIRKY_TEXT"],
       attributes["KONEC_UZAVIRKY_TEXT"]
     ]
    }
  })
end

d = Downloader.new('uzavirky', 1, 'OBJECTID,INVEST,ZABNAZ,ZACDAT,KONDAT,POZNAMKA2_2DEL,POZNAMKA3_2DEL,POLOZKA_NAZEV,LOKALIZACE,JEDCIS')
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s: %s<br> %s<br> %s - %s" % [
       attributes['CISLO_JEDNACI'],
       attributes['NAZEV_AKCE'],
       attributes['POPIS'],
       attributes["POCATEK_UZAVIRKY_TEXT"],
       attributes["KONEC_UZAVIRKY_TEXT"]
     ]
    }
  })
end


d = Downloader.new('uzavirky', 2)
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s <br> %s - %s" % [
       attributes['MSG_MTXT'],
       (attributes["MSG_MTIME_TSTA"]),
       (attributes["MSG_MTIME_TSTO"])
     ]
    }
  })
end

d = Downloader.new('kvp_vyznamne', 0, 'OBJECTID,INVEST,ZABNAZ,ZACDAT,KONDAT,POZNAMKA2_2DEL,POZNAMKA3_2DEL,POLOZKA_NAZEV,LOKALIZACE,JEDCIS')
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s: %s <br> %s - %s" % [
       attributes['JEDCIS'],
       attributes['ZABNAZ'],
       Time.at(attributes['ZACDAT'].to_i/1000),
       Time.at(attributes['KONDAT'].to_i/1000),
     ]
    }
  })
end


d = Downloader.new('kvp_vyznamne', 1, 'OBJECTID,INVEST,ZABNAZ,ZACDAT,KONDAT,POZNAMKA2_2DEL,POZNAMKA3_2DEL,POLOZKA_NAZEV,LOKALIZACE,JEDCIS')
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s: %s <br> %s - %s" % [
       attributes['JEDCIS'],
       attributes['ZABNAZ'],
       Time.at(attributes['ZACDAT'].to_i/1000),
       Time.at(attributes['KONDAT'].to_i/1000),
     ]
    }
  })
end


d = Downloader.new('kvp_vyznamne', 2, 'OBJECTID,INVEST,ZABNAZ,ZACDAT,KONDAT,POZNAMKA2_2DEL,POZNAMKA3_2DEL,POLOZKA_NAZEV,LOKALIZACE,JEDCIS')
File.open("#{d.map}_#{d.layer_index}.json", 'w+') do |f|
  f.puts JSON.dump(d.to_geojson { |attributes|
    {
     snippets: "%s: %s <br> %s - %s" % [
       attributes['JEDCIS'],
       attributes['ZABNAZ'],
       Time.at(attributes['ZACDAT'].to_i/1000),
       Time.at(attributes['KONDAT'].to_i/1000),
     ]
    }
  })
end





