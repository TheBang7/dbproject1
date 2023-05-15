package org.example;

import com.alibaba.fastjson.JSON;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.example.Post;

public class Main {
    public static void main(String[] args) {
        try {
//            String jsonStrings = Files.readString(Path.of("src/project data and scripts/posts.json"));
//            Type postListType = new TypeToken<List<Post>>(){}.getType();
//            List<Post> posts = new Gson().fromJson(jsonStrings, postListType);
//            posts.forEach(System.out::println);

            String jsonStrings2 = Files.readString(Path.of("src/project data and scripts/replies.json"));
            Type postListType2 = new TypeToken<List<Replies>>(){}.getType();
            List<Replies> replies = new Gson().fromJson(jsonStrings2, postListType2);
            replies.forEach(System.out::println);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
