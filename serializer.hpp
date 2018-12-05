#include <appbase/application.hpp>
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <unordered_map>
#include <fc/variant.hpp>

namespace eosio {


class serializer
{
public:
   serializer(const fc::microseconds& abi_serializer_max_time, const boost::filesystem::path& filename)
   :abi_serializer_max_time(abi_serializer_max_time) {
      if ( boost::filesystem::exists(filename) && !boost::filesystem::is_empty(filename) ) {
         boost::filesystem::ifstream ifs(filename);
         boost::archive::binary_iarchive ia(ifs);
         std::unordered_map<uint64_t, std::string> tmp_map;
         ia >> tmp_map;

         for ( auto& a : tmp_map) {
            std::vector<char> v(a.second.begin(), a.second.end());
            abi_cache_map.emplace( account_name(a.first), fc::raw::unpack<abi_def>(v) );
         }
         ilog("reload abi cache, size: ${s}", ("s", abi_cache_map.size()));

         ifs.close();
         boost::filesystem::remove(filename);
      }
      ofs.reset( new boost::filesystem::ofstream(filename) );
   }

   ~serializer() {
      ilog("archive abi cache data to file, size: ${s}", ("s", abi_cache_map.size()));
      boost::archive::binary_oarchive oa(*ofs);
      std::unordered_map<uint64_t, std::string> tmp_map;
   
      for ( auto& a : abi_cache_map) {
         std::vector<char> v = fc::raw::pack(a.second);
         tmp_map.emplace( a.first.value, std::string(v.begin(), v.end()) );
      }

      oa << tmp_map;
   }

   template<typename T>
   fc::variant to_variant_with_abi( const T& obj ) {
      fc::variant pretty_output;
      abi_serializer::to_variant( obj, pretty_output,
                                  [&]( account_name n ) { return get_abi_serializer( n ); },
                                  abi_serializer_max_time );
      return pretty_output;
   }

   void upsert_abi_cache( const account_name &name, const abi_def& abi );

private:

   optional<abi_serializer> get_abi_serializer( const account_name &name );
   optional<abi_serializer> abi_def_to_serializer( const account_name &name, const abi_def& abi );

   std::unordered_map<account_name, abi_def> abi_cache_map;
   fc::microseconds abi_serializer_max_time;

   std::unique_ptr<boost::filesystem::ofstream> ofs;

   boost::shared_mutex cache_mtx;
};

optional<abi_serializer> serializer::get_abi_serializer(const account_name &name) {
   if( name.good()) {
      try {
         boost::shared_lock<boost::shared_mutex> lock(cache_mtx);
         auto itr = abi_cache_map.find( name );
         if( itr != abi_cache_map.end() ) {
            return abi_def_to_serializer(name, itr->second);
         }
      } FC_CAPTURE_AND_LOG((name))
   }
   return optional<abi_serializer>();
}

optional<abi_serializer> serializer::abi_def_to_serializer( const account_name &name, const abi_def& abi ) {
   if( name.good()) {
      try {
         abi_serializer abis;

         if( name == chain::config::system_account_name ) {
            // redefine eosio setabi.abi from bytes to abi_def
            // Done so that abi is stored as abi_def in elasticsearch instead of as bytes
            abi_def abi_new = abi;
            auto itr = std::find_if( abi_new.structs.begin(), abi_new.structs.end(),
                                       []( const auto& s ) { return s.name == "setabi"; } );
            if( itr != abi_new.structs.end() ) {
               auto itr2 = std::find_if( itr->fields.begin(), itr->fields.end(),
                                          []( const auto& f ) { return f.name == "abi"; } );
               if( itr2 != itr->fields.end() ) {
                  if( itr2->type == "bytes" ) {
                     itr2->type = "abi_def";
                     // unpack setabi.abi as abi_def instead of as bytes
                     abis.add_specialized_unpack_pack( "abi_def",
                           std::make_pair<abi_serializer::unpack_function, abi_serializer::pack_function>(
                                 []( fc::datastream<const char*>& stream, bool is_array, bool is_optional ) -> fc::variant {
                                    EOS_ASSERT( !is_array && !is_optional, chain::elasticsearch_exception, "unexpected abi_def");
                                    chain::bytes temp;
                                    fc::raw::unpack( stream, temp );
                                    return fc::variant( fc::raw::unpack<abi_def>( temp ) );
                                 },
                                 []( const fc::variant& var, fc::datastream<char*>& ds, bool is_array, bool is_optional ) {
                                    EOS_ASSERT( false, chain::elasticsearch_exception, "never called" );
                                 }
                           ) );
                  }
               }
            }
            abis.set_abi( abi_new, abi_serializer_max_time );
         } else {
            abis.set_abi( abi, abi_serializer_max_time );
         }

         return abis;
      } FC_CAPTURE_AND_LOG((name))
   }
   return optional<abi_serializer>();
}

void serializer::upsert_abi_cache( const account_name &name, const abi_def& abi ) {
   if( name.good()) {
      boost::unique_lock<boost::shared_mutex> lock(cache_mtx);
      auto it = abi_cache_map.find(name);
      if( it != abi_cache_map.end() ) {
         it->second = abi;
      } else {
         abi_cache_map.emplace( name, abi );
      }

      size_t size = abi_cache_map.size();
      if ( abi_cache_map.size() % 10000 == 0)
         ilog( "abi_cache_map size: ${s}", ("s", size) );
   }
}

}