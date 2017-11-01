package org.eminem.hbase.observer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Demonip on 2017/9/11.
 */
public class ConstantUtils {

    public static List<String> getIntegKey () {
        List<String> keys = new ArrayList<>();
        keys.add("eprincitype");
        keys.add("einduclassi");
        return  keys;
    }

    public static List<String> getFloKey () {
        List<String> keys = new ArrayList<>();
        keys.add("ratio");
        keys.add("eregcap");
        keys.add("investment");
        return  keys;
    }

}
