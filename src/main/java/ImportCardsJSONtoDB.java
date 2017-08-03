import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;

public class ImportCardsJSONtoDB {

    public static void main(String[] args) {

        JSONParser parser = new JSONParser();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            FileReader f = new FileReader("MySQL.config");
            JSONObject json =  (JSONObject)parser.parse(f);
            Connection con= DriverManager.getConnection(
                    String.valueOf(json.get("url")),String.valueOf(json.get("user")),String.valueOf(json.get("pass")));
            String dir = "cards/";
            File dir2= new File(dir);
            for(File file : dir2.listFiles()){
                FileReader fr = new FileReader(dir + file.getName());
                BufferedReader br = new BufferedReader(fr);
                String fullJSON = "";
                String s;
                while((s = br.readLine()) != null){
                    fullJSON = fullJSON + s;
                }
                fullJSON = fullJSON.replaceAll("},\\{", "}|,|{").replaceAll("\\\\\"", "");
                String[] cards = fullJSON.split("\\|,\\|");
                ArrayList<Integer> sets= getSets(con);
                for(String card : cards){
                    StringReader sr = new StringReader(card);
                    System.out.println(card);
                    JSONObject jsonObject =  (JSONObject)parser.parse(sr);
                    createCards(jsonObject,con,sets);
                }
            }

            con.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static ArrayList<Integer> getSets(Connection con){
        Statement stmt;
        ArrayList<Integer> sets = new ArrayList<>();
        try {
            stmt = con.createStatement();
            String setQuery = "Select set_id from duelystcentral.sets";
            ResultSet rs = stmt.executeQuery(setQuery);
            while(rs.next()){
                sets.add(rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sets;

    }

    private static void createCards(JSONObject jsonObject, Connection con, ArrayList<Integer> sets){
        int factionID;
        int rarityID;
        int id = Integer.valueOf(String.valueOf(jsonObject.get("id")));
        int cardSetID = Integer.valueOf(String.valueOf(jsonObject.get("cardSetId")));
        String cardSetName = String.valueOf(jsonObject.get("cardSetName"));
        String faction = String.valueOf(jsonObject.get("faction"));
        String rarity = String.valueOf(jsonObject.get("rarity"));
        String name = String.valueOf(jsonObject.get("name"));
        String description = String.valueOf(jsonObject.get("description"));
        int manaCost = Integer.valueOf(String.valueOf(jsonObject.get("manaCost")));
        String type = String.valueOf(jsonObject.get("type"));
        String race = String.valueOf(jsonObject.get("race"));
        int attack;
        if(jsonObject.get("attack") != null) {
            attack = Integer.valueOf(String.valueOf(jsonObject.get("attack")));
        }
        else{
            attack = -1;
        }
        int health;
        if(jsonObject.get("health") != null) {
            health = Integer.valueOf(String.valueOf(jsonObject.get("health")));
        }
        else{
            health = -1;
        }

        switch(faction){
            case "Lyonar Kingdoms": factionID = 1;
                break;
            case "Songhai Empire": factionID = 2;
                break;
            case "Vetruvian Imperium": factionID = 3;
                break;
            case "Abyssian Host": factionID = 4;
                break;
            case "Magmar Aspects": factionID = 5;
                break;
            case "Vanar Kindred": factionID = 6;
                break;
            case "Neutral": factionID = 7;
                break;
            default: factionID = 0;
        }

        switch(rarity){
            case "Basic": rarityID = 1;
                break;
            case "Common": rarityID = 2;
                break;
            case "Rare": rarityID = 3;
                break;
            case "Epic": rarityID = 4;
                break;
            case "Legendary": rarityID = 5;
                break;
            case "Token": rarityID = 6;
                break;
            default: rarityID = 0;
        }
        Statement stmt;
        try{
            if(!sets.contains(cardSetID)){
                String setInsertQuery = "INSERT INTO `duelystcentral`.`sets`\n" +
                        "(`set_id`,\n" +
                        "`set_name`)\n" +
                        "VALUES\n" +
                        "(" + cardSetID + ",\n" +
                        "\"" + cardSetName + "\")\n" +
                        "ON DUPLICATE KEY UPDATE `set_name` = \"" + cardSetName + "\";";
                stmt = con.createStatement();
                stmt.executeUpdate(setInsertQuery);
            }
            String cardInsertQuery = "INSERT INTO `duelystcentral`.`cards`\n" +
                    "(`card_id`,\n" +
                    "`card_set_id`,\n" +
                    "`card_faction_id`,\n" +
                    "`card_rarity_id`,\n" +
                    "`card_name`,\n" +
                    "`card_description`,\n" +
                    "`card_mana_cost`,\n" +
                    "`card_type`,\n" +
                    "`card_race`,\n" +
                    "`card_attack`,\n" +
                    "`card_health`)\n" +
                    "VALUES\n" +
                    "(" + id + ",\n" +
                    cardSetID + ",\n" +
                    factionID + ",\n" +
                    rarityID + ",\n" +
                    "\"" + name + "\",\n" +
                    "\"" + description + "\",\n" +
                    manaCost + ",\n" +
                    "\"" + type + "\",\n" +
                    "\"" + race + "\",\n" +
                    attack + ",\n" +
                    health + ")\n" +
                    "ON DUPLICATE KEY UPDATE `card_set_id` = " + cardSetID + ",\n" +
                    "`card_faction_id` = " + factionID + ",\n" +
                    "`card_rarity_id` = " + rarityID + ",\n" +
                    "`card_name` = \"" + name + "\",\n" +
                    "`card_description` = \"" + description + "\",\n" +
                    "`card_mana_cost` = " + manaCost + ",\n" +
                    "`card_type` = \"" + type + "\",\n" +
                    "`card_race` = \"" + race + "\",\n" +
                    "`card_attack` = " + attack + ",\n" +
                    "`card_health` = " + health + ";";
            stmt = con.createStatement();
            stmt.executeUpdate(cardInsertQuery);
        }catch(Exception e){ System.out.println(e);}
    }

}
