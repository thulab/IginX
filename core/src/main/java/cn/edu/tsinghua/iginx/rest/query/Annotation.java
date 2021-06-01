package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class Annotation
{
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Annotation.class);
    private ObjectMapper mapper = new ObjectMapper();
    List<String> tags = new ArrayList<>();
    String text;
    String title;
    Long timestamp;
    Annotation(String str, Long tim)
    {

        timestamp = tim;
        try
        {
            JsonNode node = mapper.readTree(str);
            if (node == null)
                return;
            JsonNode text = node.get("text");
            if (text != null)
            {
                this.text = text.asText();
            }
            JsonNode title = node.get("title");
            if (title != null)
            {
                this.title = title.asText();
            }
            JsonNode tags = node.get("tags");
            if (tags != null && tags.isArray())
            {
                for (JsonNode tagsnode : tags)
                {
                    this.tags.add(tagsnode.asText());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            LOGGER.error("Wrong annotation form in database");
        }

    }

    public boolean isEqual(Annotation p)
    {
        if (p.text.compareTo(text) != 0)
            return false;
        if (p.title.compareTo(title) != 0)
            return false;
        if (p.tags.size() != tags.size())
            return false;
        for (int i = 0; i < p.tags.size(); i++)
        if (p.tags.get(i).compareTo(tags.get(i)) != 0)
        {
            return false;
        }
        return true;
    }
}
