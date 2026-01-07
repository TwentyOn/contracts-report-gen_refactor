import io
import os
import zipfile
from zipfile import ZipFile

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import RealDictCursor
import dotenv
from minio_client import MinIOClient

from generate_report_files.soprovod_generator import generate_soprovod
from generate_report_files.act_generator import generate_act
from generate_report_files.statement_generator import generate_vedomost
from generate_report_files.screen_ads.ad_screenshots_generator import generate_screens_ads
from generate_report_files.presentation.presentation_generator import generate_presentation
from generate_report_files.media_plan.mediaplan_generator import generate_mediaplan
from generate_report_files.report_generator import word_report_generate

dotenv.load_dotenv()

# Параметры подключения к БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'your_database'),
    'user': os.getenv('DB_USER', 'your_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'port': os.getenv('DB_PORT', '5432')
}


def connect_to_db():
    """Подключение к базе данных с обработкой ошибок"""
    error = None
    for _ in range(3):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print(f"Ошибка подключения к БД: {e}")
            continue
    print('Критическая ошибка подключения к БД.')
    raise error


def get_report_by_id(report_id):
    """
    Получение отчёта по ID
    :param report_id:
    :return:
    """
    conn: connection = connect_to_db()
    cur: cursor = conn.cursor()
    try:
        schema = os.getenv('DB_SCHEMA')
        query = f"""
            SELECT r.id, r.id_contracts, r.id_requests, c.number_contract, c.subject_contract, req.campany_yandex_direct
            FROM {schema}.reports r
            JOIN {schema}.contracts c ON r.id_contracts = c.id
            JOIN {schema}.requests req ON r.id_requests = req.id
            WHERE r.id = {report_id} AND (r.is_deleted = false OR r.is_deleted IS NULL)
            """

        cur.execute(query)
        conn.commit()
        report = cur.fetchone()
        report = {
            'id': report[0],
            'id_contracts': report[1],
            'id_requests': report[2],
            'number_contract': report[3],
            'subject_contract': report[4],
            'campaign_ids': report[5]  # JSONB с id кампаний
        }
        return report

    finally:
        cur.close()


def write_status(report_id: int, value: int, message: str = None):
    """
    Записывает статус выполнения отчёта в БД
    :param report_id: ID отчёта
    :param value: ID статуса
    :return:
    """
    conn: connection = connect_to_db()
    cur: cursor = conn.cursor()
    try:
        schema = os.getenv('DB_SCHEMA')
        if message:
            query = f"UPDATE {schema}.reports SET id_status = '{value}', message='{message}' WHERE id={report_id};"
        else:
            query = f"UPDATE {schema}.reports SET id_status = '{value}' WHERE id={report_id};"

        cur.execute(query)
        conn.commit()

    finally:
        cur.close()


class FileFormatter:
    """
    Класс для создаия файлов-отчётов
    """

    def __init__(self, report_id, minio_client):
        self.report_id = report_id
        self.selected_params = None
        self.param_funcs = {
            'select_content_report': word_report_generate,
            'select_screenshots_ads': generate_screens_ads,
            'select_machine_media_statement': generate_vedomost,
            'select_presentation_keys': generate_presentation,
            'select_media_plan': generate_mediaplan,
            'select_cover_letter': generate_soprovod,
            'select_act': generate_act
        }

        self.minio_client = minio_client

        self.db_conn: connection = self.connect_to_db()
        self.get_selected_params()

    def get_selected_params(self):
        schema = os.getenv('DB_SCHEMA')
        query = f"""
            SELECT select_content_report, select_screenshots_ads, select_machine_media_statement,
            select_presentation_keys, select_media_plan, select_cover_letter, select_act
            FROM {schema}.reports
            WHERE id={self.report_id}
            """

        cur: cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query)
        self.selected_params = cur.fetchone()

    def connect_to_db(self):
        """Подключение к базе данных с обработкой ошибок"""
        error = None
        print('Подключение к базе данных')
        for _ in range(3):
            try:
                print(f'Подключение к БД. Попытка {_+1}')
                conn: connection = psycopg2.connect(**DB_CONFIG)
                return conn
            except Exception as e:
                print(f"Ошибка подключения к БД: {e}")
                continue
        print('Критическая ошибка подключения к БД.')
        raise error

    def all_reports_zip_create(self, report_id: int, *args):
        """
        Упаковывает файлы отчётов в zip-архив
        :param args: объекты файлов типа BytesIO в виде кортежа (file, filename)
        :return:
        """
        output_file = io.BytesIO()
        output_filename = f'{report_id}/all_reports.zip'

        with ZipFile(output_file, 'w') as zfile:
            for file, filename in args:
                file.seek(0)
                filename = filename.split('/')[-1]
                zfile.writestr(filename, file.read())
        output_file.seek(0)

        return output_file, output_filename

    def create_files_by_params(self):
        files_to_create = filter(lambda param: self.selected_params[param], self.selected_params)
        create_files = []
        try:
            for param in files_to_create:
                file, filename = self.param_funcs[param](self.report_id)
                s3_file_path = self.upload_to_s3(file, filename, self.minio_client)
                self.write_s3path_to_bd(self.report_id, os.getenv(self.get_colname_by_param(param)), s3_file_path)
                create_files.append((file, filename))
            zipfile, zipfile_name = self.all_reports_zip_create(self.report_id, *create_files)
            s3_path = self.upload_to_s3(zipfile, zipfile_name, self.minio_client)
            self.write_s3path_to_bd(self.report_id, os.getenv('ALL_REPORT_ZIP'), s3_path)
        except Exception as err:
            print('Ошибка при создании файла')
            raise err

    def get_colname_by_param(self, param):
        param_to_colname = {
            'select_cover_letter': 'SOPROVOD_COL_NAME',
            'select_media_plan': 'MEDIAPLAN_COL_NAME',
            'select_act': 'ACT_COL_NAME',
            'select_presentation_keys': 'PRESENTATION_COL_NAME',
            'select_machine_media_statement': 'VEDOMOST_COL_NAME',
            'select_content_report': 'CONTENT_REPORT_COL_NAME',
            'select_screenshots_ads': 'SCREENSHOTS_COL_NAME'
        }
        return param_to_colname.get(param)

    def upload_to_s3(self, file, file_name, minio_client=None):
        """
        Отправляет файл в S3-хранилище
        :param minio_client:
        :param file:
        :param file_name: имя файла - критически важно чтобы содержало ID отчёта для которого файл создан (19/filename.docx)
        :return:
        """
        if not minio_client:
            minio_client = MinIOClient()
            minio_client.connect()
        error = None
        for _ in range(3):
            try:
                s3_report_path = os.getenv('S3_REPORT_PATH')
                output_path = '/'.join((s3_report_path, file_name))
                minio_client.upload_memory_file(output_path, file, len(file.getvalue()))
                print(f'Файл отправлен в хранилище: {output_path}')
                return output_path
            except Exception as e:
                print(f'Ошибка отправки отчёта (попытка {_ + 1}) {file_name} в хранилище: {e}')
                error = e
                continue
        print('Критическая ошибка отправки отчёта')
        raise error

    def write_s3path_to_bd(self, report_id: int, column: str, s3_path: str):
        """
        Записывает путь к файлу в S3-хранилище в БД
        :param report_id: ID отчёта
        :param column: имя колонки в таблице reports
        :param s3_path:
        :return:
        """
        conn: connection = connect_to_db()
        cur: cursor = conn.cursor()
        try:
            schema = os.getenv('DB_SCHEMA')
            query = f"UPDATE {schema}.reports SET {column} = '{s3_path}' WHERE id={report_id};"

            cur.execute(query)
            conn.commit()

        finally:
            cur.close()

    def close_connect(self):
        if self.db_conn:
            self.db_conn.close()
        print('Соединение закрыто.')


if __name__ == '__main__':
    client = MinIOClient()
    client.connect()
    a = FileFormatter(19, client)
    a.connect_to_db()
    print(a.db_conn)
