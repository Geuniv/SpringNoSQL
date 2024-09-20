package kopo.poly.persistance.redis.impl;

import kopo.poly.dto.MelonDTO;
import kopo.poly.persistance.redis.IMelonCacheMapper;
import kopo.poly.util.DateUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Component
public class MelonCacheMapper implements IMelonCacheMapper {

    private final RedisTemplate<String, Object> redisDB;

    // 수집된 노래 저장하기
    @Override
    public int insertSong(List<MelonDTO> pList, String redisKey) throws Exception {

        log.info(this.getClass().getName() + ".insertSong Start !");

        int res;

        // Redis에 저장될 키
        String key = "MELON_" + DateUtil.getDateTime("yyyyMMdd");

        redisDB.setKeySerializer(new StringRedisSerializer());
        redisDB.setValueSerializer(new Jackson2JsonRedisSerializer<>(MelonDTO.class));

        // 람다식으로 데이터 저장하기
        pList.forEach(melon -> redisDB.opsForList().leftPush(key, melon));

        // 저장된 데이터는 1시간동안 보관하기
        redisDB.expire(key, 1, TimeUnit.HOURS);

        res = 1;

        log.info(this.getClass().getName() + ".insertSong End !");

        return res;
    }

    // RedisDB에 저장된 데이터가 존재하는지 확인하기 ( 키 값 유무로 데이터 저장 여부 체크함 )
    @Override
    public boolean getExistKey(String redisKey) throws Exception {
        return redisDB.hasKey(redisKey);
    }

    @Override
    public List<MelonDTO> getSongList(String redisKey) throws Exception {

        log.info(this.getClass().getName() + ".getSongList Start !");

        redisDB.setKeySerializer(new StringRedisSerializer());
        redisDB.setValueSerializer(new Jackson2JsonRedisSerializer<>(MelonDTO.class));

        List<MelonDTO> rList = null;

        // 저장된 키가 존재한다면...
        if (redisDB.hasKey(redisKey)) {
            rList = (List) redisDB.opsForList().range(redisKey, 0, -1);
        }

        // 저장된 데이터는 1시간동안 연장하기
        redisDB.expire(redisKey, 1, TimeUnit.HOURS);

        log.info(this.getClass().getName() + ".getSongList End !");

        return rList;
    }
}
