package com.crimsonhexagon.rsm.lettuce;

import io.lettuce.core.codec.RedisCodec;
import org.apache.catalina.util.CustomObjectInputStream;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Extension of {@link SerializationCodec} to use tomcat's {@link CustomObjectInputStream} with the {@link ClassLoader} provided
 */
class ContextClassloaderJdkSerializationCodec implements RedisCodec<String, Object> {
    private final Log log = LogFactory.getLog(getClass());
    private final ClassLoader containerClassLoader;

    public ContextClassloaderJdkSerializationCodec(ClassLoader containerClassLoader) {
        this.containerClassLoader = containerClassLoader;
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return StandardCharsets.UTF_8.decode(bytes).toString();
    }

    @Override
    public Object decodeValue(ByteBuffer bytes) {
        try {
            byte[] array = new byte[bytes.remaining()];
            bytes.get(array);
            ByteArrayInputStream bais = new ByteArrayInputStream(array);
            final ObjectInputStream ois;
            if (containerClassLoader != null) {
                ois = new CustomObjectInputStream(bais, containerClassLoader);
            } else {
                ois = new ObjectInputStream(bais);
            }
            return ois.readObject();
        } catch (Exception e) {
            log.error("Failed to decode value", e);
            return null;
        }
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return StandardCharsets.UTF_8.encode(key);
    }

    @Override
    public ByteBuffer encodeValue(Object value) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bytes);
            os.writeObject(value);
            return ByteBuffer.wrap(bytes.toByteArray());
        } catch (IOException e) {
            log.error("Failed to encode value", e);
            return null;
        }
    }
}