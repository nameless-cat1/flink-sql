package hkust;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Node implements Serializable {

    public String type;
    public Object key;
    public List<Object> attribute_value;
    public List<String> attribute_name;
//    public Long timestamp;


    public Node() {
    }

    public Node(Node node) {
        this.type = node.type;
        this.key = node.key;
        this.attribute_value = new ArrayList<>();
        this.attribute_value.addAll(node.attribute_value);
        this.attribute_name = new ArrayList<>();
        this.attribute_name.addAll(node.attribute_name);
    }
    public Node(String type, Object key, List<Object> attribute_value, List<String> attribute_name) {
        this.type = type;
        this.key = key;
        this.attribute_value = new ArrayList<>();
        this.attribute_value.addAll(attribute_value);
        this.attribute_name = new ArrayList<>();
        this.attribute_name.addAll(attribute_name);
    }

    public Node(List<Object> attribute_value, List<String> attribute_name) {
        this.type ="Tmp";
        this.key =0;
        this.attribute_value = new ArrayList<>();
        this.attribute_value.addAll(attribute_value);
        this.attribute_name = new ArrayList<>();
        this.attribute_name.addAll(attribute_name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() == this.getClass()){
//            if (!((Node) obj).key.equals(this.key)){
//                return false;
//            }

            for (int i=0;i<attribute_name.size();i++) {
                int index = ((Node) obj).attribute_name.indexOf(attribute_name.get(i));
                if (index==-1){
                    return false;
                }
                if (!(((Node) obj).attribute_value.get(index).equals(attribute_value.get(i)))){
                    return false;
                }

            }
            return true;

        }else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(attribute_value);
    }


    public void setKey(String nextKeys){
        key=getValueByName(nextKeys);
    }

    public Object getValueByName(String name){
        int index = attribute_name.indexOf(name);
        return index==-1?null:attribute_value.get(index);
    }


    @Override
    public String toString() {
        return "Node{" +
                "type='" + type + '\'' +
                ", key=" + key +
                ", attribute_value=" + attribute_value +
                ", attribute_name=" + attribute_name +
                '}';
    }
}
