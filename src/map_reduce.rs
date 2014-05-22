extern crate collections;
use collections::HashMap;

pub type PairList = Vec<Pair<~str,uint>>;

#[deriving(Clone)]
pub struct Mapper {
    pub f: fn(~str, ~str) -> PairList
}

#[deriving(Clone)]
pub struct Reducer {
    pub f: fn (PairList) -> PairList
}


pub fn MapReduce(
    mapper: Mapper,
    reducer: Reducer,
    data: Vec<Pair<~str,~str>>
        ) -> PairList {

    Reduce(reducer, Shuffle(Map(mapper, &data)))
}


#[deriving(Show, Clone)]
pub struct Pair<K,V> {
    pub key: K,
    pub value: V
}

fn Map(mapper: Mapper, data: &Vec<Pair<~str,~str>>) -> Vec<PairList> {

    let (tx, rx) = channel();

    for ref pair in data.iter() {

        let ctx = tx.clone();
        let func = mapper.f.clone();

        let key : ~str = pair.key.clone();
        let value : ~str = pair.value.clone();

        spawn(proc() {
            ctx.send(func(key, value));
        });
    }

    let mut result : Vec<PairList> = vec!();
    for _ in range(0, data.len()) {
        result.push(rx.recv());
    }

    return result;
}


fn Reduce(reducer: Reducer, data: Vec<PairList>) -> PairList {

    let (tx, rx) = channel();

    for pair_list in data.iter() {

        let ctx = tx.clone();
        let func = reducer.f.clone();
        let bin = pair_list.clone();

        spawn(proc() {
            ctx.send(func(bin));
        });

    }

    let mut result : PairList = vec!();
    for _ in range(0, data.len()) {
        result.push_all(rx.recv().as_slice());
    }

    return result;

}


fn Shuffle(data: Vec<PairList>) -> Vec<PairList> {

    let map = find_indexes_for_bins(&data);
    return create_bins(map, data);

}


fn find_indexes_for_bins(data: &Vec<PairList>) -> HashMap<char,uint> {

    // Descide number of bins and indexes
    let mut counter = 0;
    let mut map : HashMap<char,uint> = HashMap::new();
    for partial in data.iter() {
        for pair in partial.iter() {
            let ch = pair.key.char_at(0);
            match map.find_mut(&ch) {
                Some(_) => {},
                None => {
                    map.insert(ch, counter);
                    counter += 1;
                }
            }
        }
    }

    return map;
}

fn create_bins(index_map: HashMap<char, uint>, data: Vec<PairList>) -> Vec<PairList> {

    let mut bins : Vec<PairList> = Vec::from_fn(index_map.len(), |_| vec!());
        for partial in data.iter() {
        for pair in partial.iter() {

            let ch = pair.key.char_at(0);
            let idx = index_map.get(&ch);

            bins.get_mut(*idx).push(pair.clone());

        }
    }

    return bins;
}
