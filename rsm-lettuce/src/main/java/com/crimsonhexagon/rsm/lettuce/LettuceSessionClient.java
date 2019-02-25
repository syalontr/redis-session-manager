package com.crimsonhexagon.rsm.lettuce;

import com.crimsonhexagon.rsm.RedisSession;
import com.crimsonhexagon.rsm.RedisSessionClient;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class LettuceSessionClient implements RedisSessionClient {
    private final Log log = LogFactory.getLog(getClass());
    private final StatefulRedisConnection<String, Object> connection;
    private final RedisCodec<String, Object> codec;
    
    public LettuceSessionClient(StatefulRedisConnection<String, Object> connection, RedisCodec<String, Object> codec) {
        this.connection = connection;
        this.codec = codec;
    }
    
    @Override
    public void save(String key, RedisSession session) {
        connection.sync().set(key, session);
    }

    @Override
    public RedisSession load(String key) {
        Object obj = connection.sync().get(key);
        if (obj != null) {
            if (RedisSession.class.isAssignableFrom(obj.getClass())) {
                return RedisSession.class.cast(obj);
            } else {
                log.warn("Incompatible session class found in redis for session [" + key + "]: " + obj.getClass());
                delete(key);
            }
        }
        return null;
    }

    @Override
    public void delete(String key) {
        connection.sync().del(key);
    }

    @Override
    public void expire(String key, long expirationTime, TimeUnit timeUnit) {
        connection.async().pexpire(key, TimeUnit.MILLISECONDS.convert(expirationTime, timeUnit));
    }

    @Override
    public boolean exists(String key) {
        Long count = connection.sync().exists(key);
        return count != null && count.longValue() == 1L;
    }

    @Override
    public int getEncodedSize(Object obj) {
        ByteBuffer bb = codec.encodeValue(obj);
        return bb == null ? 0 : bb.remaining();
    }

    @Override
    public void shutdown() {
        connection.close();
    }

}
