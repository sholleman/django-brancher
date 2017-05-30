from StringIO import StringIO

from django.conf import settings
from django.db import connection
from subprocess import check_output

from model_utils import Choices

# noinspection PyInterpreter


class DbNameMixin(object):
    _branch_name = None
    _db_name = None

    DB_ENGINES = Choices(
        'postgresql',
        'mysql'
    )

    def add_arguments(self, parser):
        parser.add_argument('--branch_name', help="This overrides the git branch as the branch name")

    @property
    def full_branched_db_name(self):
        return '{}_{}'.format(self.get_db_name, self.branch_name)

    @property
    def get_db_name(self):
        if self._db_name is None:
            self._db_name = settings.DATABASES['default']['NAME']
        return self._db_name

    @property
    def branch_name(self):
        if self._branch_name is None:
            self._branch_name = check_output('git rev-parse --abbrev-ref HEAD', shell=True).strip()
        return self._branch_name

    def change_defaults(self, branch_name=None, **options):
        self._branch_name = branch_name

    def create_database(self):
        with connection.cursor() as cursor:
            if self.is_postgresql:
                cursor.execute("CREATE DATABASE {branch} template {name};".format(branch=self.full_branched_db_name, name=self.get_db_name))
            elif self.is_mysql:
                data = StringIO()
                data.write(cursor.execute())
                # dump 'default' database
                    with open(filename, 'w') as f:
                        cmd = ['mysqldump',
                               '--user={}'.format(db_user),
                               '--password={}'.format(db_pass),
                               '{}'.format(DATABASES['default']['NAME']+'__master')]
                        f.write(subprocess.check_output(cmd))

                # create new database
                cmd = ['mysql',
                       '--user={}'.format(db_user),
                       '--password={}'.format(db_pass),
                       '--execute=CREATE DATABASE {};'.format(db_name)]
                subprocess.check_call(cmd)

                # mysql import dump.sql to new db
                with open(filename, 'r') as f:
                    cmd = ['mysql',
                           '--user={}'.format(db_user),
                           '--password={}'.format(db_pass),
                           db_name]
                    subprocess.check_call(cmd, stdin=f)

    def drop_database(self):
        with connection.cursor() as cursor:
            if self.is_postgresql:
                cursor.execute("DROP DATABASE IF EXISTS {database};".format(database=self.full_branched_db_name))
            elif self.is_mysql:
                raise NotImplementedError()

    @property
    def db_engine(self):
        if settings.DATABASES['default']['ENGINE'] == 'django.db.backends.mysql':
            return self.DB_ENGINES.mysql
        elif settings.DATABASES['default']['ENGINE'] == 'django.db.backends.postgresql':
            return self.DB_ENGINES.postgresql
        else:
            raise Exception('Not Implemented')

    @property
    def is_postgresql(self):
        return self.db_engine == self.DB_ENGINES.postgresql

    @property
    def is_mysql(self):
        return self.db_engine == self.DB_ENGINES.mysql
