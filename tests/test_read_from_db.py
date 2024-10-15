import pandas as pd
from sqlalchemy import create_engine

def test_read_from_db():
    db_user = 'postgres'
    db_password = 'postgres'
    db_host = '192.168.56.135'
    db_port = '5432'
    db_name = 'house_predictions_target'

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    sql = """
    SELECT 
        foundation_brktil, foundation_cblock, foundation_pconc, foundation_slab, 
        foundation_stone, foundation_wood, kitchenqual_ex, kitchenqual_fa, 
        kitchenqual_gd, kitchenqual_ta, totrmsabvgrd, wooddecksf, yrsold, 
        "1stFlrSF"
    FROM target_predictions
    """ 
    df = pd.read_sql(sql, engine)
    assert not df.empty, "No data retrieved from database"