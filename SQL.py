#!/usr/bin/env python
# coding: utf-8

# # SQL

# ### Описание проекта:
# 
# Коронавирус застал мир врасплох, изменив привычный порядок вещей. В свободное время жители городов больше не выходят на улицу, не посещают кафе и торговые центры. Зато стало больше времени для книг. Это заметили стартаперы — и бросились создавать приложения для тех, кто любит читать.
# 
# Ваша компания решила быть на волне и купила крупный сервис для чтения книг по подписке. Ваша первая задача как аналитика — проанализировать базу данных.
# В ней — информация о книгах, издательствах, авторах, а также пользовательские обзоры книг. Эти данные помогут сформулировать ценностное предложение для нового продукта.

# ### Описание данных
# 
# **Таблица `books`**
# 
# Содержит данные о книгах:
# 
# - `book_id` — идентификатор книги;
# - `author_id` — идентификатор автора;
# - `title` — название книги;
# - `num_pages` — количество страниц;
# - `publication_date` — дата публикации книги;
# - `publisher_id` — идентификатор издателя.
# 
# **Таблица `authors`**
# 
# Содержит данные об авторах:
# 
# - `author_id` — идентификатор автора;
# - `author` — имя автора.
# 
# **Таблица `publishers`**
# 
# Содержит данные об издательствах:
# 
# - `publisher_id` — идентификатор издательства;
# - `publisher` — название издательства;
# 
# **Таблица `ratings`**
# 
# Содержит данные о пользовательских оценках книг:
# 
# - `rating_id` — идентификатор оценки;
# - `book_id` — идентификатор книги;
# - `username` — имя пользователя, оставившего оценку;
# - `rating` — оценка книги.
# 
# **Таблица `reviews`**
# 
# Содержит данные о пользовательских обзорах на книги:
# 
# - `review_id` — идентификатор обзора;
# - `book_id` — идентификатор книги;
# - `username` — имя пользователя, написавшего обзор;
# - `text` — текст обзора.

# ### Задания
# 
# - Посчитайте, сколько книг вышло после 1 января 2000 года;
# - Для каждой книги посчитайте количество обзоров и среднюю оценку;
# - Определите издательство, которое выпустило наибольшее число книг толще 50 страниц — так вы исключите из анализа брошюры;
# - Определите автора с самой высокой средней оценкой книг — учитывайте только книги с 50 и более оценками;
# - Посчитайте среднее количество обзоров от пользователей, которые поставили больше 50 оценок.

# In[1]:


# импортируем библиотеки
import pandas as pd
from sqlalchemy import create_engine
# устанавливаем параметры
db_config = {'user': 'praktikum_student', # имя пользователя
'pwd': 'Sdf4$2;d-d30pp', # пароль
'host': 'rc1b-wcoijxj3yxfsf3fs.mdb.yandexcloud.net',
'port': 6432, # порт подключения
'db': 'data-analyst-final-project-db'} # название базы данных
connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
 db_config['pwd'],
 db_config['host'],
 db_config['port'],
 db_config['db'])
# сохраняем коннектор
engine = create_engine(connection_string, connect_args={'sslmode':'require'})


# In[2]:


query ='''

SELECT * 
FROM books
LIMIT 5

'''
pd.io.sql.read_sql(query, con = engine)


# In[3]:


query ='''

SELECT * 
FROM authors
LIMIT 5

'''
pd.io.sql.read_sql(query, con = engine)


# In[4]:


query ='''

SELECT * 
FROM publishers
LIMIT 5

'''
pd.io.sql.read_sql(query, con = engine)


# In[5]:


query ='''

SELECT * 
FROM ratings
LIMIT 5

'''
pd.io.sql.read_sql(query, con = engine)


# In[6]:


query ='''

SELECT * 
FROM reviews
LIMIT 5

'''
pd.io.sql.read_sql(query, con = engine)


# **1.Посчитайте, сколько книг вышло после 1 января 2000 года**

# In[7]:


query ='''

SELECT COUNT(book_id) AS books_cnt
FROM books
WHERE publication_date > '2000-01-01'
'''
pd.io.sql.read_sql(query, con = engine)


# После 1 января 2000 года вышло 819 книг

# **2.Для каждой книги посчитайте количество обзоров и среднюю оценку**

# In[8]:


query ='''

SELECT b.title,
       COUNT(DISTINCT re.review_id) AS count, 
       AVG(ra.rating) AS avg_rating
FROM books as b
LEFT JOIN ratings AS ra ON ra.book_id = b.book_id
LEFT JOIN reviews AS re ON re.book_id = b.book_id
GROUP BY b.book_id
ORDER BY count DESC
LIMIT 10
'''
pd.io.sql.read_sql(query, con = engine)


# На книгу Harry Potter and the Prisoner of Azkaban вышло 6 обзоров с средней оценкой 4.4, наибольшее количество обзоров получила книга Twilight (Twilight #1), однако средняя оценка 3.7

# **3.Определите издательство, которое выпустило наибольшее число книг толще 50 страниц**

# In[9]:


query ='''

SELECT p.publisher,
       COUNT(b.book_id) AS books_count
FROM publishers AS p
LEFT JOIN books AS b ON  b.publisher_id = p.publisher_id 
WHERE num_pages > 50
GROUP BY p.publisher
ORDER BY books_count DESC
LIMIT 1
'''
pd.io.sql.read_sql(query, con = engine)


# Penguin Books оказалось издательством, выпустившим наибольшее число книг толще 50 страниц

# **4.Определите автора с самой высокой средней оценкой книг — учитывайте только книги с 50 и более оценками**

# In[10]:


query ='''
SELECT a.author AS author,
       b.title,
       AVG(r.rating) AS avg_rating
FROM authors AS a
JOIN books AS b ON a.author_id = b.author_id
JOIN ratings AS r ON r.book_id = b.book_id
GROUP BY a.author, b.title
HAVING COUNT(r.rating) >= 50
LIMIT 10
'''
pd.io.sql.read_sql(query, con = engine)


# In[11]:


query ='''

SELECT tab.author,
       AVG(tab.avg_rating)
FROM (SELECT a.author AS author,
             b.title,
             AVG(r.rating) AS avg_rating
      FROM authors AS a
      JOIN books AS b ON a.author_id = b.author_id
      JOIN ratings AS r ON r.book_id = b.book_id
      GROUP BY a.author, b.title
      HAVING COUNT(r.rating) >= 50) AS tab
GROUP BY tab.author
ORDER BY AVG(tab.avg_rating) DESC
LIMIT 1
'''
pd.io.sql.read_sql(query, con = engine) 


# J.K. Rowling/Mary GrandPré оказались с самой высокой средней оценкой книг 

# **5.Посчитайте среднее количество обзоров от пользователей, которые поставили больше 50 оценок**

# In[12]:


query ='''
SELECT username
FROM ratings
GROUP BY username
HAVING COUNT(rating_id)>50
'''
pd.io.sql.read_sql(query, con = engine) 


# In[13]:


query ='''
SELECT COUNT(review_id) AS count_review
FROM reviews
WHERE username IN (SELECT username
                   FROM ratings
                   GROUP BY username
                   HAVING COUNT(rating_id)>50)
'''
pd.io.sql.read_sql(query, con = engine) 


# In[14]:


query ='''
SELECT AVG(tab.count) AS avg_review
FROM (SELECT COUNT(review_id) AS count
      FROM reviews
      WHERE username IN (SELECT username
                         FROM ratings
                         GROUP BY username
                         HAVING COUNT(rating_id)>50)
      GROUP BY username) AS tab
'''
pd.io.sql.read_sql(query, con = engine) 


# 24.3 - среднее кол-во обзоров от пользователей, которые поставили больше 50 оценок

# ### Выводы
# 
# * После 1 января 2000 года вышло 819 книг
# * На книгу Harry Potter and the Prisoner of Azkaban вышло 6 обзоров с средней оценкой 4.4, наибольшее количество обзоров получила книга Twilight (Twilight #1), однако средняя оценка 3.7
# * Penguin Books оказалось издательством, выпустившим наибольшее число книг толще 50 страниц
# * J.K. Rowling/Mary GrandPré оказались с самой высокой средней оценкой книг
# * 24.3 - среднее кол-во обзоров от пользователей, которые поставили больше 50 оценок

# In[ ]:




