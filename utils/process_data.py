# coding: utf-8
"""
@author: linwenxing
@contact: linwx.mail@gmail.com
"""
import os
import pandas as pd


def merge_data(files_path, data_1):
    file_list = os.listdir(files_path)
    for file_name in file_list[1:]:
        file_path = os.path.join(files_path, file_name)
        print(file_name)
        data_2 = pd.read_csv(file_path, encoding='utf8', usecols=['电影', '类型', '导演', '主演'])
        data_1 = pd.concat([data_1, data_2], axis=0, ignore_index=True, sort=True)  # 将df2数据与df1合并
    data_1 = data_1.drop_duplicates()  # 去重
    data_1 = data_1.reset_index(drop=True)  # 重新生成index
    data_1.to_csv(files_path + '/' + 'total.csv', columns=['电影', '类型', '导演', '主演'])  # 将结果保存为新的csv文件


def generate_id(begin_num, cnt, id_name):
    id_list = []
    for i in range(begin_num, begin_num + cnt):
        id_list.append(i)
    return pd.DataFrame(data=id_list, columns=[id_name])


def duplicate_people(datas, column_name):
    duplicate_list = []
    for data in datas:
        try:
            duplicate_list.extend(data.split('/'))
        except:
            pass
    duplicate_list = list(set(duplicate_list))
    duplicate_list_cnt = len(duplicate_list)
    duplicate_df = pd.DataFrame(data=duplicate_list, columns=[column_name])
    return duplicate_df, duplicate_list_cnt


def process_every_cols(total_data_path):
    total_data = pd.read_csv(total_data_path, encoding='utf8')
    data_film = total_data['电影']
    data_movie_type = total_data['类型']
    data_director = total_data['导演']
    data_actor = total_data['主演']

    film_list = list(data_film)
    film_cnt = len(film_list)
    data_film_name = pd.DataFrame(data=film_list, columns=['film_name'])
    data_director_name, director_cnt = duplicate_people(datas=data_director, column_name='director_name')
    data_movie_type_name, movie_type_cnt = duplicate_people(datas=data_movie_type, column_name='movie_type_name')
    data_actor_name, actor_cnt = duplicate_people(datas=data_actor, column_name='actor_name')

    data_film_id = generate_id(begin_num=10001, cnt=film_cnt, id_name='file_id')
    data_director_id = generate_id(begin_num=20001, cnt=director_cnt, id_name='director_id')
    data_movie_type_id = generate_id(begin_num=30001, cnt=movie_type_cnt, id_name='movie_type_id')
    data_actor_id = generate_id(begin_num=40001, cnt=actor_cnt, id_name='actor_id')

    # 拼接成节点数据
    film = pd.concat([data_film_id, data_film_name], axis=1)
    film['label'] = '电影'
    director = pd.concat([data_director_id, data_director_name], axis=1)
    director['label'] = '导演'
    actor = pd.concat([data_actor_id, data_actor_name], axis=1)
    actor['label'] = '演员'
    movie_type = pd.concat([data_movie_type_id, data_movie_type_name], axis=1)
    movie_type['label'] = '类型'

    # 生成节点文件
    film.columns = ['index:ID', 'film', ':LABEL']
    director.columns = ['index:ID', 'director', ':LABEL']
    actor.columns = ['index:ID', 'actor', ':LABEL']
    movie_type.columns= ['index:ID', 'movie_type', ':LABEL']

    film.to_csv('film.csv', index=False, encoding='utf-8_sig',columns=['index:ID', 'film', ':LABEL'])
    director.to_csv('director.csv', index=False, encoding='utf-8_sig',columns =['index:ID', 'director', ':LABEL'])
    actor.to_csv('actor.csv', index=False, encoding='utf-8_sig',columns = ['index:ID', 'actor', ':LABEL'])
    movie_type.to_csv('movie_type.csv', index=False, encoding='utf-8_sig',columns = ['index:ID', 'movie_type', ':LABEL'])

    df = pd.read_csv('../datas/total.csv', encoding='utf8')
    df_film = pd.read_csv('film.csv', encoding='utf8')
    df_director = pd.read_csv('director.csv', encoding='utf8')
    df_actor = pd.read_csv('actor.csv', encoding='utf8')
    df_movie_type = pd.read_csv('movie_type.csv', encoding='utf8')

    df = df.fillna('未知')

    director_films, actor_films, director_actors, film_types = [], [], [], []

    for index, row in df.iterrows():
        film_name = row['电影']
        director = row['导演']
        actor = row['主演']
        movie_type = row['类型']
        directorList = director.split('/')
        actorList = actor.split('/')
        typeList = movie_type.split('/')



        filmID = df_film['index:ID'].loc[df_film['film'] == film_name].values[0]

        try:
            for dir in directorList:
                directorID = df_director['index:ID'].loc[df_director['director'] == dir].values[0]
                director_film = [directorID, filmID, '导演', '导演']
                director_films.append(director_film)

            for act in actorList:
                actorID = df_actor['index:ID'].loc[df_actor['actor'] == act].values[0]
                actor_film = [actorID, filmID, '出演', '出演']
                actor_films.append(actor_film)

            for dir in directorList:
                directorID = df_director['index:ID'].loc[df_director['director'] == dir].values[0]
                for act in actorList:
                    actorID = df_actor['index:ID'].loc[df_actor['actor'] == act].values[0]
                    director_actor = [directorID, actorID, '合作', '合作']
                    director_actors.append(director_actor)

            for movie_type in typeList:
                typeID = df_movie_type['index:ID'].loc[df_movie_type['movie_type'] == movie_type].values[0]

                film_type = [filmID, typeID, '类型', '类型']
                film_types.append(film_type)
        except:
            print(row)


    df_director_film = pd.DataFrame(data = director_films,columns=[':START_ID',':END_ID','relation',':TYPE'])
    df_actor_film = pd.DataFrame(data = actor_films,columns=[':START_ID',':END_ID','relation',':TYPE'])
    df_director_actor = pd.DataFrame(data = director_actors,columns=[':START_ID',':END_ID','relation',':TYPE'])
    df_film_type = pd.DataFrame(data = film_types,columns=[':START_ID',':END_ID','relation',':TYPE'])

    df_director_film.to_csv('relation_director_film.csv', index=False, encoding='utf-8_sig')
    df_actor_film.to_csv('relation_actor_film.csv', index=False, encoding='utf-8_sig')
    df_director_actor.to_csv('relation_director_actor.csv', index=False, encoding='utf-8_sig')
    df_film_type.to_csv('relation_film_type.csv', index=False, encoding='utf-8_sig')



if __name__ == '__main__':
    data_1 = pd.read_csv('../datas/movies_华语.csv', encoding='utf8', usecols=['电影', '类型', '导演', '主演'])

    merge_data('../datas', data_1)

    process_every_cols('../datas/total.csv')
