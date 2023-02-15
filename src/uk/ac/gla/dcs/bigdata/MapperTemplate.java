package uk.ac.gla.dcs.bigdata;

import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/19
 */
@Mapper
public interface MapperTemplate {

    MapperTemplate INSTANCE = Mappers.getMapper(MapperTemplate.class);

    // VOTemplate Entity2VO(EntityTemplate entity);
    // EntityTemplate DTO2Entity(DTOTemplate dto);
}
