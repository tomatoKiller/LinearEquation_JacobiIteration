package LinearEquation;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by wu on 14-7-23.
 */
public class XmlTest {
    public static void main(String[] args) throws IOException {
        Document document = DocumentHelper.createDocument();
        Element root = DocumentHelper.createElement("Student");
        document.setRootElement(root);

        Element root2 = DocumentHelper.createElement("Student");
        Document document2 = DocumentHelper.createDocument(root2);

        root2.addAttribute("name", "zhangsan");

        Element helloElement = root2.addElement("hello");
        helloElement.setText("hello Text");

        Element worldElement = root2.addElement("world");
        worldElement.setText("world Text");

        XMLWriter xmlWriter = new XMLWriter();
        xmlWriter.write(document2);

        OutputFormat output = new OutputFormat("    ", true);
        XMLWriter xmlWriter1 = new XMLWriter(new FileOutputStream("student.xml"), output);
        xmlWriter1.write(document2);
    }
}
