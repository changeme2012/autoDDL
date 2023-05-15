package com.lc.app.interfac;

import java.util.List;

/**
 * ClassName:KafkaInterface
 * Package:com.lc.app
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-10:53
 */
public interface KafkaInterface {


    void qurey ();

//    void create ();

    void exclude();

    void include();

    void spliceSQL();

    void allcolumn();

    List<String> function();

    void encapsulatedSQL();

    void save();

    String where ();

    void join ();

    String lookpuJoin();



    void print();





}
