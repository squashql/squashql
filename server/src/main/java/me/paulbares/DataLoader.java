package me.paulbares;

import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 */
public class DataLoader {

  static List<String> headers() {
    return List.of("ean", "pdv", "categorie", "type marque", "sensibilite", "quantite", "prix", "achat", "score",
            "min marche");
  }

  static List<Object[]> dataBase() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Hyper", 100, 4.9d, 3, 50, 4d}
    );
  }

  static List<Object[]> dataMDDBaisse() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Hyper", 100, 4.5d, 3, 50, 4d}
    );
  }

  static List<Object[]> dataMDDBaisseSimuSensi() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Basse", 100, 4d, 3, 50, 4d}
    );
  }

  public static SparkDatastore createTestDatastoreWithData() {
    var ean = new Field("ean", String.class);
    var pdv = new Field("pdv", String.class);
    var categorie = new Field("categorie", String.class);
    var type = new Field("type-marque", String.class);
    var sensi = new Field("sensibilite", String.class);
    var quantite = new Field("quantite", Integer.class);
    var prix = new Field("prix", Double.class);
    var achat = new Field("achat", Integer.class);
    var score = new Field("score-visi", Integer.class);
    var minMarche = new Field("min-marche", Double.class);

    Column qCol = col(quantite.name());
    Column pCol = col(prix.name());
    Column sCol = col(score.name());
    SparkDatastore datastore = new SparkDatastore(
            List.of(ean, pdv, categorie, type, sensi, quantite, prix, achat, score, minMarche),
            qCol.multiply(pCol).as("ca"),
            qCol.multiply(pCol.minus(functions.col(achat.name()))).as("marge"),
            pCol.divide(functions.col(minMarche.name())).multiply(sCol).as("numerateur-indice"),
            col("numerateur-indice").divide(sCol).as("indice-prix"));

    datastore.load(Datastore.MAIN_SCENARIO_NAME, dataBase());
    datastore.load("mdd-baisse", dataMDDBaisse());
    datastore.load("mdd-baisse-simu-sensi", dataMDDBaisseSimuSensi());

    return datastore;
  }
}