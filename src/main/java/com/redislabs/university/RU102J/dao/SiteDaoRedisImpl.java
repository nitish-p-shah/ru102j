package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    public Site findByKey(String key){
        try(Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    @Override
    public Site findById(long id) {
        String key = RedisSchema.getSiteHashKey(id);
        return findByKey(key);
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        // START Challenge #1
        Set<Site> sites = new HashSet<>();
        try(Jedis jedis = jedisPool.getResource()) {
            String idsKey = RedisSchema.getSiteIDsKey();
            Set<String> keys = jedis.smembers(idsKey);
            for (String key : keys){
                Site site = findByKey(key);
                if(site != null) sites.add(site);
            }
        }
        return sites;
        // return Collections.emptySet();
        // END Challenge #1
    }
}
