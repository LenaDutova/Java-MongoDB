import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * File - Project Structure - Libraries
 * Add New Project Library (+) - From Maven
 * Search org.mongodb:mongodb-driver-sync:5.5.1
 * Ok - Ok - Apply
 *
 * Check it in External libraries
 */
public class LibraryConnector {

    // Строка подключения к локальному серверу MongoDB (ваш компьютер)
    static String connectionStringLocal = "mongodb://localhost:27017";

    // Строка подключения к удаленного серверу MongoDB (по IP)
    static String connectionStringRemote = "mongodb://10.242.137.74:27017";

    public static void main(String[] args) {
        String connectionString = connectionStringLocal;
        String databaseName = "digital_library";
        String collectionInDatabaseName = "books";

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            // Получаем доступ к нашей базе данных
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            // Получаем доступ к коллекции книг
            MongoCollection<Document> collection = database.getCollection(collectionInDatabaseName);

            // TODO
//            findSamples(collection);
//            updateSamples(collection);

            // FIXME
            refresh(collection);
        }
    }

    // region // Перезапись коллекции

    private static void refresh (MongoCollection<Document> collection) {
        // Удалить "старые" документы
        collection.drop();

        // Создать список "новых" документов
        ArrayList<Document> books = new ArrayList<>();
        books.add(new Document("title", "Мастер и Маргарита")
                .append("author", "Михаил Булгаков")
                .append("genre", "Роман")
                .append("publish_year", 1967)
                .append("pages", 480)
                .append("tags", Arrays.asList("мистика", "сатира")));
        books.add(new Document("title", "Преступление и наказание")
                .append("author", "Федор Достоевский")
                .append("genre", "Роман")
                .append("publish_year", 1866)
                .append("pages", 671)
                .append("tags", Arrays.asList("классика", "психология")));
        books.add(new Document("title", "Собачье сердце")
                .append("author", "Михаил Булгаков")
                .append("genre", "Повесть")
                .append("publish_year", 1925)
                .append("pages", 112)
                .append("tags", Arrays.asList("фантастика", "сатира")));
        books.add(new Document("title", "Мертвые души")
                .append("author", "Николай Гоголь")
                .append("genre", "Поэма")
                .append("publish_year", 1842)
                .append("pages", 352)
                .append("tags", Arrays.asList("классика", "сатира")));
        books.add(new Document("title", "Отцы и дети")
                .append("author", "Иван Тургенев")
                .append("genre", "Роман")
                .append("publish_year", 1862)
                .append("pages", 208)
                .append("tags", Arrays.asList("классика", "реализм")));
        books.add(new Document("title", "Война и мир")
                .append("author", "Лев Толстой")
                .append("genre", "Роман-эпопея")
                .append("publish_year", 1869)
                .append("pages", 1300)
                .append("tags", Arrays.asList("история", "классика")));
        books.add(new Document("title", "Герой нашего времени")
                .append("author", "Михаил Лермонтов")
                .append("genre", "Роман")
                .append("publish_year", 1840)
                .append("pages", 224)
                .append("tags", Arrays.asList("классика", "психология")));

        // Добавить все "новые" документы
        collection.insertMany(books);
        findAllBooks(collection);
    }

    // endregion

    // region // Создание, обновление и удаление книг в коллекции

    private static void updateSamples(MongoCollection<Document> collection) {
        createBook (collection);
        updateBook(collection);
        deleteBook(collection);
    }

    private static void createBook(MongoCollection<Document> collection) {
        System.out.println("--- 1. Добавляем новую книгу ---");

        // Создание нового документа
        Document newBook = new Document("title", "Маленький принц")
                .append("author", "Антуан де Сент-Экзюпери")
                .append("publish_year", 1943)
                .append("genre", "Притча")
                .append("pages", 96);

        // Добавление в коллекцию единственного документа
        InsertOneResult result = collection.insertOne(newBook);
        System.out.println("Книга 'Маленький принц' успешно добавлена: " + result.getInsertedId());

        // Проверим, что данные изменились
        findAllBooks(collection);
    }

    private static void updateBook(MongoCollection<Document> collection) {
        System.out.println("\n--- 2. Обновляем жанр книги 'Мертвые души' ---");

        // Определяем фильтр и операцию изменения
        Bson filter = Filters.eq("title", "Мертвые души");
        Bson update = Updates.set("genre", "Роман-поэма");

        // Обновление единственного документа
        UpdateResult result = collection.updateOne(filter, update);
        System.out.println("Совпало документов: " + result.getMatchedCount() + ", модифицировано документов: " + result.getModifiedCount());

        // Проверим, что данные изменились
        Document updatedBook = collection.find(filter).first();
        System.out.println(updatedBook != null ? ("Обновленный документ: " + updatedBook.toJson()) : "Документ не найден.");
    }

    private static void deleteBook (MongoCollection<Document> collection){
        System.out.println("\n--- 3. Удаляем книгу 'Герой нашего времени' ---");

        // Определяем фильтр
        Bson filter = Filters.eq("title", "Герой нашего времени");

        // Удаление единственного документа
        DeleteResult result = collection.deleteOne(filter);
        System.out.println("Удалено документов: " + result.getDeletedCount());

//        // Или удаление единственного документа с демонстрацией (подтверждение заказа :)
//        Document deleted = collection.findOneAndDelete(filter);
//        System.out.println(deleted != null ? ("Удален документ: " + deleted.toJson()) : "Удаляемый документ не найден.");

        // Проверим, что книга действительно удалена:
        System.out.print("Пытаемся найти удаленную книгу: ");
        Document deletedBook = collection.find(filter).first();
        if (deletedBook == null) {
            System.out.println("Книга успешно удалена, найти не удалось.");
        } else System.out.println("Остались книги с названием 'Герой нашего времени'");
    }

    // endregion


    // region // Поиск книг в коллекции

    private static void findSamples(MongoCollection<Document> collection){
        findAllBooks(collection);
        findBook(collection);
        findBooksByPublishYear(collection);
    }

    private static void findAllBooks (MongoCollection<Document> collection){
        System.out.println("\n--- 1. Все книги в коллекции ---");

        // Находим все документы в коллекции
        FindIterable<Document> allBooks = collection.find();
        // collection.find().sort(new Document("publish_year", 1)) // сортировка по возрастанию
        // collection.find().limit(2)   // ограниченное количество книг

        // Итерируемся по результатам и выводим их
        for (Document doc : allBooks) {
            System.out.println(doc.toJson());
        }
    }

    private static void findBook (MongoCollection<Document> collection){
        System.out.println("\n--- 2. Найти книгу 'Собачье сердце'---");

        // Находим первый документ по точному совпадению (equals)
        Document specificBook = collection.find(Filters.eq("title", "Собачье сердце")).first();

        // Если такой найден, то выводим его содержимое
        if (specificBook != null) {
            System.out.println(specificBook.toJson());
        }
    }

    private static void  findBooksByPublishYear (MongoCollection<Document> collection){
        System.out.println("\n--- 3. Найти все книги, опубликованные после 1870 года ---");

        // Находим документы, используя фильтр "больше чем" (greater than)
        FindIterable<Document> recentBooks = collection.find(Filters.gt("publish_year", 1870));

        // Итерируемся по результатам и выводим каждый документ
        for (Document doc : recentBooks) {
            System.out.println(doc.toJson());
        }
    }

    // endregion
}
