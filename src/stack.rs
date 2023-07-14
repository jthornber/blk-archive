use generic_array::{ArrayLength, GenericArray};
use std::ptr;

//-----------------------------------

// A fixed size stack that handles rotations and duplications
pub struct Stack<T: Default + Copy, N: ArrayLength<T> + ArrayLength<u8>> {
    values: GenericArray<T, N>,
    indexes: GenericArray<u8, N>,
}

impl<T: Default + Copy, N: ArrayLength<T> + ArrayLength<u8>> Default for Stack<T, N> {
    fn default() -> Self {
        let mut r = Self {
            values: GenericArray::default(),
            indexes: GenericArray::default(),
        };

        for i in 0..r.indexes.len() {
            r.indexes[i] = i as u8;
        }

        r
    }
}

impl<T: Default + Copy, N: ArrayLength<T> + ArrayLength<u8>> Stack<T, N> {
    pub fn get(&self, index: usize) -> &T {
        self.values.get(self.indexes[index] as usize).unwrap()
    }

    pub fn get_mut(&mut self, index: usize) -> &mut T {
        self.values.get_mut(self.indexes[index] as usize).unwrap()
    }

    pub fn rot(&mut self, index: usize) {
        let tmp = self.indexes[index];

        unsafe {
            ptr::copy(self.indexes.as_ptr(), self.indexes[1..].as_mut_ptr(), index);
        }

        self.indexes[0] = tmp;
    }

    pub fn dup(&mut self, index: usize) {
        let last = self.indexes[self.indexes.len() - 1];
        let v = self.values[self.indexes[index] as usize];

        unsafe {
            ptr::copy(
                self.indexes.as_ptr(),
                self.indexes[1..].as_mut_ptr(),
                self.indexes.len() - 1,
            );
        }

        self.indexes[0] = last;
        self.values[last as usize] = v;
    }
}

#[cfg(test)]
mod list_tests {
    use super::*;
    use generic_array::typenum::U4;

    type TestStack = Stack<u32, U4>;

    fn new_stack() -> TestStack {
        let mut s = TestStack::default();
        for i in 0..4 {
            let v = s.get_mut(i);
            *v = i as u32;
        }
        assert_stack(&s, 0, 1, 2, 3);
        s
    }

    fn print_stack(s: &TestStack) {
        eprintln!("[{}, {}, {}, {}]", s.get(0), s.get(1), s.get(2), s.get(3));
    }

    fn assert_stack(s: &TestStack, n1: u32, n2: u32, n3: u32, n4: u32) {
        assert_eq!(*s.get(0), n1);
        assert_eq!(*s.get(1), n2);
        assert_eq!(*s.get(2), n3);
        assert_eq!(*s.get(3), n4);
    }

    #[test]
    fn test_create() {
        let _s = Stack::<u32, U4>::default();
    }

    #[test]
    fn test_rot() {
        let mut s = new_stack();

        s.rot(2);
        assert_stack(&s, 2, 0, 1, 3);

        s.rot(3);
        assert_stack(&s, 3, 2, 0, 1);

        s.rot(0);
        assert_stack(&s, 3, 2, 0, 1);
    }

    #[test]
    fn test_dup() {
        let mut s = new_stack();

        s.dup(2);
        assert_stack(&s, 2, 0, 1, 2);

        s.dup(3);
        assert_stack(&s, 2, 2, 0, 1);

        s.dup(0);
        assert_stack(&s, 2, 2, 2, 0);

        print_stack(&s);
        *s.get_mut(0) = 7;
        print_stack(&s);
        assert_stack(&s, 7, 2, 2, 0);
    }
}
