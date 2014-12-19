package com.petuum.ps.netty.protobuf;

/**
 * Created by Yuxin Su on 2014/12/19.
 */
public class AddressBook {

    public static void main(String[] args) {
        AddressBookProtos.Person person = AddressBookProtos.Person.newBuilder()
                .setEmail("yxsu@cse")
                .setName("yxsu")
                .setId(10).build();
        System.out.println(person.getName());
        System.out.println(person.getEmail());
        System.out.println(person.getSerializedSize());
    }
}
