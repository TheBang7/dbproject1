package org.example;

import com.google.gson.annotations.SerializedName;
import java.util.List;

public class Post {
    @SerializedName("Post ID")
    private int postID;
    @SerializedName("Title")
    private String title;
    @SerializedName("Category")
    private List<String> category;
    @SerializedName("Content")
    private String content;
    @SerializedName("Posting Time")
    private String postingTime;
    @SerializedName("Posting City")
    private String postingCity;
    @SerializedName("Author")
    private String author;
    @SerializedName("Author Registration Time")
    private String authorRegistrationTime;
    @SerializedName("Author's ID")
    private String authorID;
    @SerializedName("Author's Phone")
    private String authorPhone;
    @SerializedName("Authors Followed By")
    private List<String> authorFollowedBy;
    @SerializedName("Authors Who Favorited the Post")
    private List<String> authorFavorite;
    @SerializedName("Authors Who Shared the Post")
    private List<String> authorShared;
    @SerializedName("Authors Who Liked the Post")
    private List<String> authorLiked;

    @Override
    public String toString() {
        return "Post{" +
                "postID=" + postID +
//                ", title='" + title + '\'' +
//                ", category=" + category +
//                ", content='" + content + '\'' +
//                ", postingTime='" + postingTime + '\'' +
//                ", postingCity='" + postingCity + '\'' +
                ", Author='" + author + '\'' +
//                ", authorRegistrationTime='" + authorRegistrationTime + '\'' +
                ", authorID='" + authorID + '\'' +
//                ", authoPhone='" + authorPhone + '\'' +
//                ", authorFollowedBy=" + authorFollowedBy +
//                ", authorFavorite=" + authorFavorite +
//                ", authorShared=" + authorShared +
//                ", authorLiked=" + authorLiked +
                '}';
    }

    public int getPostID() {
        return postID;
    }

    public void setPostID(int postID) {
        this.postID = postID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getCategory() {
        return category;
    }

    public void setCategory(List<String> category) {
        this.category = category;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getPostingTime() {
        return postingTime;
    }

    public void setPostingTime(String postingTime) {
        this.postingTime = postingTime;
    }

    public String getPostingCity() {
        return postingCity;
    }

    public void setPostingCity(String postingCity) {
        this.postingCity = postingCity;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        author = author;
    }

    public String getAuthorRegistrationTime() {
        return authorRegistrationTime;
    }

    public void setAuthorRegistrationTime(String authorRegistrationTime) {
        this.authorRegistrationTime = authorRegistrationTime;
    }

    public String getAuthorID() {
        return authorID;
    }

    public void setAuthorID(String authorID) {
        this.authorID = authorID;
    }

    public String getAuthorPhone() {
        return authorPhone;
    }

    public void setAuthorPhone(String authoPhone) {
        this.authorPhone = authoPhone;
    }

    public List<String> getAuthorFollowedBy() {
        return authorFollowedBy;
    }

    public void setAuthorFollowedBy(List<String> authorFollowedBy) {
        this.authorFollowedBy = authorFollowedBy;
    }

    public List<String> getAuthorFavorite() {
        return authorFavorite;
    }

    public void setAuthorFavorite(List<String> authorFavorite) {
        this.authorFavorite = authorFavorite;
    }

    public List<String> getAuthorShared() {
        return authorShared;
    }

    public void setAuthorShared(List<String> authorShared) {
        this.authorShared = authorShared;
    }

    public List<String> getAuthorLiked() {
        return authorLiked;
    }

    public void setAuthorLiked(List<String> authorLiked) {
        this.authorLiked = authorLiked;
    }
}
