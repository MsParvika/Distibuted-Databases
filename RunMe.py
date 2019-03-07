DATABASE_NAME = 'distributeDb'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

import psycopg2
import traceback
import testHelper
import Interface as RunFile

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            [result, e] = testHelper.testloadratings(RunFile, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print "loadratings function pass!"
            else:
                print "loadratings function fail!"

            [result, e] = testHelper.testrangepartition(RunFile, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print "rangepartition function pass!"
            else:
                print "rangepartition function fail!"

            [result, e] = testHelper.testrangeinsert(RunFile, RATINGS_TABLE, 100, 2, 3, conn, '2')
            if result:
                print "rangeinsert function pass!"
            else:
                print "rangeinsert function fail!"

            testHelper.deleteAllPublicTables(conn)
            RunFile.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            [result, e] = testHelper.testroundrobinpartition(RunFile, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print "roundrobinpartition function pass!"
            else:
                print "roundrobinpartition function fail"

            [result, e] = testHelper.testroundrobininsert(RunFile, RATINGS_TABLE, 100, 1, 3, conn, '0')
            if result :
                print "roundrobininsert function pass!"
            else:
                print "roundrobininsert function fail!"

            choice = raw_input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()
