package com.avro;

import avro.domain.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestDemo {

    public void create(){
        User user = new User();
        user.setAge(24);
        user.setUsername("Ferdinand");

        User user1 = User.newBuilder().setUsername("Tom").setAge(25).build();

        User user2 = new User("Jerry",26);

        //基于某个对象来创建一个新的对象
        //底层用的clone接口（克隆机制）
        User user3 = new User().newBuilder(user1).setAge(31).build();
    }

    //序列化
    @Test
    public void write() throws IOException {
        User user = new User("Ken", 100);
        User user1 = new User("里奇",100);
        DatumWriter<User> datumWriter = new SpecificDatumWriter<User>();
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(datumWriter);

        //创建底层的文件输出通道
        dataFileWriter.create(user.getSchema(), new File("user.txt"));
        //把对象数据写到文件中
        dataFileWriter.append(user);
        dataFileWriter.append(user1);
        dataFileWriter.close();
    }

    //反序列化
    @Test
    public void read() throws IOException {
        DatumReader<User> datumReader = new SpecificDatumReader<User>();
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("user.txt"),datumReader);

        while (dataFileReader.hasNext()){
            System.out.println(dataFileReader.next());
        }
    }
}
