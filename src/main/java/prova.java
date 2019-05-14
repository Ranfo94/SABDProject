import java.util.ArrayList;
import java.util.Collections;

public class prova {

    public static void main(String[] args) {

        ArrayList<String> list = new ArrayList<String>();
        list.add("01 JavaFx");
        list.add("10 Java");
        list.add("04 WebGL");
        list.add("02 OpenCV");
        Collections.sort(list);
        System.out.println(list);

        System.out.println("prova");
    }

}
